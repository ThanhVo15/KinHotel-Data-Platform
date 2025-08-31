from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar, Literal
from datetime import datetime, timezone
import hashlib
import json
import logging

try:
    # Pydantic v2
    from pydantic import BaseModel, ValidationError  # type: ignore
    _HAS_PYDANTIC = True
except Exception:
    BaseModel = object  # type: ignore
    ValidationError = Exception  # type: ignore
    _HAS_PYDANTIC = False


TIn = TypeVar("TIn") # raw input type (e.g, dict)
TOut = TypeVar("TOut") # normalized output type (we assume dict to enrich SCD)

logger = logging.getLogger(__name__)

# ---------------- SCD config ----------------
@dataclass(frozen=True)
class SCDConfig:
    """
    Cấu hình SCD Type-2 cho transformer.
    - natural_key_fields: danh sách field tạo "khóa tự nhiên" (ổn định theo business)
    - track_fields: danh sách field cần theo dõi để tính hash thay đổi
    - effective_time_source: chọn thời điểm đặt cho scd_effective_start_at
        + "window_end": ưu tiên dùng window_end nếu có; fallback now()
        + "now": luôn dùng thời điểm hiện tại UTC
    """
    natural_key_fields: Sequence[str]
    track_fields: Sequence[str]
    effective_time_source: Literal["window_end", "now"] = "window_end"

# ---------------- Transform result ----------------

@dataclass
class TransformResult(Generic[TOut]):
    source: str
    branch_id: Optional[int]
    records_in: int
    records_out: int
    errors: List[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    data: List[TOut] = field(default_factory=list)
    status: str = "success"

    @property
    def duration_s(self) -> Optional[float]:
        if not self.finished_at:
            return None
        return (self.finished_at - self.started_at).total_seconds()
    
# ---------------- Abstract Transformer ----------------

class AbstractTransformer(ABC, Generic[TIn, TOut]):
    """
    Base transformer:
      - pre_transform / transform_record / post_transform hooks
      - Dedup theo khóa
      - Validate bằng Pydantic (nếu cung cấp schema)
      - Enrich SCD Type-2 (scd_effective_start_at, scd_effective_end_at, scd_is_current,
                          scd_natural_key, scd_change_hash)

    Thiết kế sẵn chỗ mở rộng alert (chưa bật).
    """

    def __init__(
        self,
        name: str,
        *,
        output_schema: Optional[Type[BaseModel]] = None,
        dedup_keys: Optional[Sequence[str]] = None,
        source: str = "unknown",
        scd_config: Optional[SCDConfig] = None,
        on_validation_error: Literal["drop", "raise", "keep"] = "drop",
    ) -> None:
        self.name = name
        self.output_schema = output_schema
        self.dedup_keys = tuple(dedup_keys) if dedup_keys else tuple()
        self.source = source
        self.scd_config = scd_config
        self.on_validation_error = on_validation_error
        self.logger = logging.getLogger(f"{__name__}.{name}")

        # -------- Hooks --------
    def pre_transform(self, records: Iterable[TIn]) -> Iterable[TIn]:
        """Lọc/sửa nhẹ input trước khi map từng bản ghi."""
        return records

    @abstractmethod
    def transform_record(self, record: TIn) -> Optional[TOut]:
        """Map 1 bản ghi raw -> output chuẩn hóa (dict). Trả None để bỏ qua."""
        raise NotImplementedError

    def post_transform(self, records: List[TOut]) -> List[TOut]:
        """Chạy bước xử lý cuối cùng (vd sort, chuẩn hóa key)."""
        return records

    # -------- Dedup --------
    def _dedup(self, out: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.dedup_keys:
            return out
        seen = set()
        unique: List[Dict[str, Any]] = []
        for rec in out:
            key_vals = tuple(rec.get(k) for k in self.dedup_keys)
            if key_vals not in seen:
                seen.add(key_vals)
                unique.append(rec)
        return unique

    # -------- Validation (Pydantic) --------
    def _validate(self, out: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        if not self.output_schema or not _HAS_PYDANTIC:
            return out, []

        valid: List[Dict[str, Any]] = []
        errors: List[str] = []

        for i, rec in enumerate(out):
            try:
                # Pydantic v2: model_validate cho phép parse & validate dict
                self.output_schema.model_validate(rec)  # type: ignore
                valid.append(rec)
            except ValidationError as ve:  # type: ignore
                msg = f"row={i} validation_error={str(ve)[:500]}"
                if self.on_validation_error == "raise":
                    raise
                elif self.on_validation_error == "keep":
                    errors.append(msg)
                    valid.append(rec)
                else:  # drop
                    errors.append(msg)
        return valid, errors

    # -------- SCD helpers --------
    @staticmethod
    def _md5_of_obj(obj: Any) -> str:
        """
        Tính md5 một cách ổn định:
         - Convert obj -> json string có sort_keys
         - Hỗ trợ datetime bằng ISO format
        """
        def default(o):
            if isinstance(o, datetime):
                if o.tzinfo is None:
                    o = o.replace(tzinfo=timezone.utc)
                return o.astimezone(timezone.utc).isoformat()
            return str(o)

        payload = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=default)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    def _compute_natural_key(self, rec: Dict[str, Any], fields: Sequence[str]) -> Optional[str]:
        vals = []
        for f in fields:
            v = rec.get(f)
            if v is None or v == "":
                return None
            vals.append(str(v))
        return "|".join(vals)

    def _compute_change_hash(self, rec: Dict[str, Any], fields: Sequence[str]) -> str:
        subset = {k: rec.get(k) for k in fields}
        return self._md5_of_obj(subset)

    def _effective_start_time(
        self,
        *,
        effective_time_source: Literal["window_end", "now"],
        window_start: Optional[datetime],
        window_end: Optional[datetime],
    ) -> datetime:
        if effective_time_source == "window_end" and window_end is not None:
            ts = window_end
        else:
            ts = datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _enrich_scd(
        self,
        out: List[Dict[str, Any]],
        *,
        window_start: Optional[datetime],
        window_end: Optional[datetime],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Thêm các cột SCD vào từng record.
        Trả về (records_enriched, warnings)
        """
        warns: List[str] = []
        if not self.scd_config:
            return out, warns

        nk_fields = self.scd_config.natural_key_fields
        tf_fields = self.scd_config.track_fields

        effective_start = self._effective_start_time(
            effective_time_source=self.scd_config.effective_time_source,
            window_start=window_start,
            window_end=window_end,
        )

        enriched: List[Dict[str, Any]] = []
        for i, rec in enumerate(out):
            # Natural key
            nk = self._compute_natural_key(rec, nk_fields)
            if not nk:
                warns.append(f"row={i} missing natural_key fields={list(nk_fields)}; record dropped")
                # Với SCD, thiếu natural key thì không thể versioning -> bỏ
                continue

            # Change hash
            ch = self._compute_change_hash(rec, tf_fields)

            r = dict(rec)
            r["scd_natural_key"] = nk
            r["scd_change_hash"] = ch
            r["scd_effective_start_at"] = effective_start
            r["scd_effective_end_at"] = None
            r["scd_is_current"] = True
            enriched.append(r)

        return enriched, warns

    # -------- Orchestrate --------
    def transform(
        self,
        records: Iterable[TIn],
        *,
        branch_id: Optional[int] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> TransformResult[Dict[str, Any]]:
        started = datetime.now(timezone.utc)
        recs_in = 0
        mapped: List[Dict[str, Any]] = []
        errors: List[str] = []

        # 1) map từng bản ghi
        for raw in self.pre_transform(records):
            recs_in += 1
            try:
                out = self.transform_record(raw)
                if out is not None:
                    if not isinstance(out, dict):
                        errors.append(f"row={recs_in-1} output is not dict -> dropped")
                    else:
                        mapped.append(out)
            except Exception as e:
                errors.append(f"row={recs_in-1} transform_error={type(e).__name__}: {e}")

        # 2) dedup
        mapped = self._dedup(mapped)

        # 3) validate bằng Pydantic
        mapped, v_errs = self._validate(mapped)
        errors.extend(v_errs)

        # 4) enrich SCD
        mapped, scd_warns = self._enrich_scd(mapped, window_start=window_start, window_end=window_end)
        errors.extend(scd_warns)  # treat missing NK là lỗi mềm (warning), nhưng gộp vào errors list

        # 5) post
        mapped = self.post_transform(mapped)

        finished = datetime.now(timezone.utc)
        status = "success" if not errors else ("partial" if mapped else "error")

        self.logger.info(
            f"🧪 Transform[{self.name}] in={recs_in} out={len(mapped)} "
            f"errors={len(errors)} dur={(finished-started).total_seconds():.2f}s"
        )

        return TransformResult[Dict[str, Any]](
            source=self.source,
            branch_id=branch_id,
            records_in=recs_in,
            records_out=len(mapped),
            errors=errors,
            data=mapped,
            status=status,
            started_at=started,
            finished_at=finished,
        )