# src/data_pipeline/core/pms_extractor.py
import asyncio
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# --- SỬA LỖI IMPORT Ở ĐÂY ---
# Import trực tiếp từ các file anh em trong cùng thư mục `core`
from ..abstract_extractor import AbstractExtractor, ExtractionResult
from ..clients.pms_client import PMSClient

# Import từ các thư mục khác
from ...config.pipeline_config import TOKEN_BRANCH_MAP
from ....utils.env_utils import get_config
from ....utils.state_manager import load_last_run_timestamp, save_last_run_timestamp
from ....utils.date_params import DateWindow, ICT

logger = logging.getLogger(__name__)

class PMSExtractor(AbstractExtractor):
    """
    Lớp Extractor cơ sở cho PMS.
    - Quản lý PMSClient.
    - Cung cấp template method cho việc extract.
    - Chứa logic trích xuất tăng trưởng (incremental) có state.
    """
    # Lấy cấu hình tập trung từ file config
    TOKEN_BRANCH_MAP = TOKEN_BRANCH_MAP
    INITIAL_START_DATE = datetime(2023, 6, 1, tzinfo=timezone.utc)

    def __init__(self, client: PMSClient):
        super().__init__("PMS")
        self.client = client

    async def close(self):
        """Đóng session của client."""
        await self.client.close()

    # --- Template Method Pattern ---
    async def _perform_extraction(self, session, branch_id: int, **kwargs) -> List[Any]:
        """
        Lớp con BẮT BUỘC phải implement phương thức này.
        Đây là nơi thực hiện logic gọi API cụ thể (ví dụ: gọi endpoint 'bookings').
        **kwargs sẽ chứa 'params' từ lớp cha truyền xuống.
        """
        raise NotImplementedError("Lớp con phải triển khai phương thức _perform_extraction")

    async def extract_async(self, *, branch_id: int, **kwargs) -> ExtractionResult:
        """
        Template method: Cung cấp khung sườn cho việc extract từ một branch.
        Nó xử lý việc tạo session, logging, và bắt lỗi chung.
        Lớp con có thể override để thêm logic, nhưng nên gọi super().
        """
        start_time = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
        source_name = f"PMS:{getattr(self, 'ENDPOINT', 'Unknown')}"

        try:
            session = await self.client.get_session(branch_id)
            raw_records = await self._perform_extraction(session, branch_id, **kwargs)
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"✅ Extracted {len(raw_records)} raw records from {branch_name} for '{source_name}' in {duration:.2f}s."
            )

            kwargs.pop('params', None)
            kwargs.pop('date_window', None)

            return ExtractionResult(
                data=raw_records, 
                source=source_name,
                branch_id=branch_id,
                branch_name=branch_name,
                record_count=len(raw_records),
                **kwargs,
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.error(f"❌ Extraction failed for {branch_name} after {duration:.2f}s: {e}", exc_info=True)
            kwargs.pop('params', None)
            kwargs.pop('date_window', None)
            return ExtractionResult(
                data=None, source=source_name, branch_id=branch_id,
                branch_name=branch_name, status="error", error=str(e), **kwargs,
            )

    # --- Logic Incremental dùng chung ---
    def _build_window_from_state(
        self, *, endpoint: str, field: str, branch_id: int, lookback_days: int
    ) -> DateWindow:
        source_key = f"{endpoint}:{field}"
        now_utc = datetime.now(timezone.utc)
        last_run_utc = load_last_run_timestamp(source=source_key, branch_id=branch_id)

        if not last_run_utc:
            self.logger.info(f"FIRST RUN for '{source_key}' on branch {branch_id}. Backfilling from {self.INITIAL_START_DATE.date()}.")
            start_utc = self.INITIAL_START_DATE
            return DateWindow.from_utc(start_utc, now_utc, field=field, tz=ICT)

        if field in ("check_in", "created_date"):
            self.logger.info(f"ROLLING LOOKBACK strategy for '{source_key}' on branch {branch_id}. Using {lookback_days} days.")
            start_utc = now_utc - timedelta(days=lookback_days)
        else: # Chiến lược DELTA cho last_updated
            self.logger.info(f"DELTA strategy for '{source_key}' on branch {branch_id}. Loading from {last_run_utc.isoformat()}.")
            start_utc = last_run_utc - timedelta(minutes=15)
        
        return DateWindow.from_utc(start_utc, now_utc, field=field, tz=ICT)

    async def extract_multi_branch(
        self, *, endpoint: str, field: str, branch_ids: Optional[List[int]] = None,
        lookback_days: int = 30, max_concurrent: int = 5, jitter_max_s: float = 0.8, **kwargs
    ) -> Dict[int, ExtractionResult]:
        
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        sem = asyncio.Semaphore(max_concurrent)
        results: Dict[int, ExtractionResult] = {}

        async def _run_one(bid: int):
            async with sem:
                try:
                    dw = self._build_window_from_state(
                        endpoint=endpoint, field=field, branch_id=bid, lookback_days=lookback_days
                    )
                    await asyncio.sleep(random.random() * jitter_max_s)
                    res = await self.extract_async(branch_id=bid, date_window=dw, **kwargs)
                    results[bid] = res
                    if res.is_success:
                        source_key = f"{endpoint}:{field}"
                        save_last_run_timestamp(source_key, bid, dw.end)
                except Exception as e:
                    results[bid] = ExtractionResult(
                        data=None, source=f"PMS:{endpoint}", branch_id=bid,
                        branch_name=self.TOKEN_BRANCH_MAP.get(bid, f"Branch {bid}"),
                        status="error", error=str(e),
                    )

        await asyncio.gather(*(_run_one(b) for b in branch_ids))
        return results