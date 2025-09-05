# src/data_pipeline/extractors/abstract_extractor.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, date
from pathlib import Path
import uuid

import pandas as pd

# Đọc config Phase 1 (không hardcode)
from ...utils.env_utils import get_config


@dataclass
class ExtractionResult:
    """Template for extraction results. Holds data and metadata."""
    data: Any
    source: str
    branch_id: Optional[int] = None
    branch_name: Optional[str] = None
    extracted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    record_count: int = 0
    status: str = "success"
    error: Optional[str] = None

    # Giữ lại các field cũ để không vỡ chỗ đang dùng
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    created_date_from: Optional[datetime] = None
    created_date_to: Optional[datetime] = None
    check_out_date: Optional[datetime] = None

    @property
    def is_success(self) -> bool:
        return self.status == "success" and self.error is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "branch_id": self.branch_id,
            "branch_name": self.branch_name,
            "extracted_at": self.extracted_at.isoformat(),
            "record_count": self.record_count,
            "status": self.status,
            "error": self.error,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "created_date_from": self.created_date_from.isoformat() if self.created_date_from else None,
            "created_date_to": self.created_date_to.isoformat() if self.created_date_to else None,
        }


class AbstractExtractor(ABC):
    """
    Base class cho mọi extractor.
    Vòng đời chuẩn (Phase 2):
      Extract (async) → (Flatten ở lớp con) → Save to Staging (template method)
    """

    def __init__(self, name: str):
        self.name = name   # ví dụ "PMS", "ODOO"
        self.logger = logging.getLogger(f"{__name__}.{name}")

        # Cache config để tránh đọc nhiều lần
        self._config = get_config()
        self._staging_dir = Path(self._config["paths"]["staging_dir"])

    # ========== Abstract bắt buộc ==========

    @abstractmethod
    async def extract_async(self, **kwargs) -> ExtractionResult:
        """Async extraction method - lớp con phải implement."""
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        """Validate extractor configuration."""
        pass

    @abstractmethod
    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        """Extract data từ nhiều branch (nếu có)."""
        pass

    @abstractmethod
    def close(self):
        """Dọn tài nguyên (session, connection, ...)."""
        pass

    # ========== Convenience sync wrapper ==========
    def extract(self, **kwargs) -> ExtractionResult:
        """Sync wrapper cho extract_async (tiện cho test nhanh)."""
        return asyncio.run(self.extract_async(**kwargs))

    # ========== Template method: Extract → Save ==========
    async def extract_and_save(
        self,
        dataset: str,
        field: str,
        *,
        partition_dt: Optional[date] = None,
        branch_id: Optional[int] = None,
        filename_prefix: Optional[str] = None,
        prefer_parquet: bool = True,
        **kwargs,
    ) -> Tuple[ExtractionResult, Optional[str]]:
        """
        Chạy extract_async(**kwargs) rồi lưu staging theo chuẩn Hive-style.

        Path staging: {STAGING_DIR}/{system}/{dataset}/dt=YYYYMMDD/{filename}.(parquet|csv)

        return: (result, output_path_str_or_None)
        """
        result = await self.extract_async(**kwargs)
        if not result.is_success:
            self.logger.error("❌ Extract failed, skip saving to staging.")
            return result, None

        try:
            output_path = self._save_to_staging(
                data=result.data,
                dataset=dataset,
                field=field,
                partition_dt=partition_dt,
                branch_id=branch_id if branch_id is not None else result.branch_id,
                filename_prefix=filename_prefix,
                prefer_parquet=prefer_parquet,
            )
            self.logger.info(f"💾 Saved to staging: {output_path}")
            return result, str(output_path)
        except Exception as e:
            self.logger.exception(f"❌ Save to staging failed: {e}")
            return ExtractionResult(
                data=result.data,
                source=result.source,
                branch_id=result.branch_id,
                branch_name=result.branch_name,
                extracted_at=result.extracted_at,
                record_count=result.record_count,
                status="error",
                error=f"save_failed: {e}",
            ), None

    # ========== Implement lưu staging chuẩn Hive ==========
    def _save_to_staging(
        self,
        *,
        data: Any,
        dataset: str,
        field: str,
        partition_dt: Optional[date] = None,
        branch_id: Optional[int] = None,
        filename_prefix: Optional[str] = None,
        prefer_parquet: bool = True,
    ) -> Path:
        """
        Lưu data vào staging theo cấu trúc:
          {STAGING_DIR}/{system}/{dataset}/dt=YYYYMMDD/{filename}.parquet (hoặc .csv)

        - system: self.name.lower() (vd: "pms")
        - dataset: ví dụ "bookings"
        - filename: {prefix or dataset}_{field}_branch={id or ALL}_extractedAt={YYYYmmdd-HHMMSSZ}_{uuid}.{ext}
        """
        system = self._sanitize(self.name.lower() if self.name else "system")
        dataset = self._sanitize(dataset)
        field = self._sanitize(field)

        # Partition date mặc định = hôm nay (UTC) để ổn định
        dt = (partition_dt or datetime.now(timezone.utc).date())
        dt_str = dt.strftime("%Y%m%d")

        out_dir = self._staging_dir / system / f"dt={dt_str}" / dataset
        out_dir.mkdir(parents=True, exist_ok=True)

        # Tạo DataFrame
        df = self._to_dataframe(data)

        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        bid = branch_id if branch_id is not None else "ALL"
        prefix = self._sanitize(filename_prefix) if filename_prefix else f"{dataset}"
        base_name = f"{prefix}_{field}_branch={bid}_extractedAt={ts}_{uuid.uuid4().hex[:8]}"

        # Ghi parquet trước, nếu fail → CSV
        if prefer_parquet:
            try:
                path = out_dir / f"{base_name}.parquet"
                self._write_parquet(df, path)
                return path
            except Exception as e:
                self.logger.warning(f"⚠️ Parquet failed ({e}); falling back to CSV.")

        path = out_dir / f"{base_name}.csv"
        self._write_csv(df, path)
        return path

    # ========== Helpers ==========
    @staticmethod
    def _sanitize(s: str) -> str:
        return "".join(ch if ch.isalnum() or ch in ("-", "_", "=") else "_" for ch in s)

    @staticmethod
    def _to_dataframe(data: Any) -> pd.DataFrame:
        """
        Chuẩn hoá data thành DataFrame:
          - Nếu đã là DataFrame -> copy
          - Nếu list[dict] -> json_normalize
          - Nếu rỗng -> DataFrame rỗng
        """
        if isinstance(data, pd.DataFrame):
            return data.copy()
        if data is None:
            return pd.DataFrame()
        if isinstance(data, list):
            if not data:
                return pd.DataFrame()
            if isinstance(data[0], dict):
                return pd.json_normalize(data, max_level=2)
            return pd.DataFrame({"value": data})
        if isinstance(data, dict):
            return pd.json_normalize([data], max_level=2)
        # fallback chung
        return pd.DataFrame({"value": [data]})

    @staticmethod
    def _write_parquet(df: pd.DataFrame, path: Path):
        # Cần pyarrow hoặc fastparquet; ưu tiên pyarrow
        df.to_parquet(path, engine="pyarrow", index=False)

    @staticmethod
    def _write_csv(df: pd.DataFrame, path: Path):
        # UTF-8 BOM để mở Excel OK
        df.to_csv(path, index=False, encoding="utf-8-sig")
