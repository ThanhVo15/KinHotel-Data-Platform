from __future__ import annotations
from typing import Dict, Any, Optional, List, Iterable
from datetime import date, datetime, timedelta, timezone
from pandas import to_datetime
from dateutil.relativedelta import relativedelta

from src.data_pipeline.transformers.abstract_transformer import (
    AbstractTransformer, TransformResult
)
from src.data_pipeline.schemas.pms_schemas import DimDate


class DimDateTransformer(AbstractTransformer[Dict[str, Any], Dict[str, Any]]):
    """
    Tạo ra một bảng DimDate hoàn chỉnh dựa trên khoảng thời gian
    của một lô dữ liệu booking.
    
    - Tìm ngày create_date sớm nhất và check_out_date muộn nhất.
    - Tạo ra một dòng cho mỗi ngày trong khoảng đó.
    """

    def __init__(self) -> None:
        super().__init__(
            name="DimDateTransformer",
            output_schema=DimDate,
            dedup_keys=("date_key",),
            source="PMS:Bookings:date_dimension", 
            scd_config=None,  # Date dimension doesn't need SCD
            on_validation_error="raise"
        )

    def _create_date_record(self, current_date: date) -> Dict[str, Any]:
        """Tạo một dictionary đầy đủ thuộc tính cho một ngày duy nhất."""
        
        # Quarter calculation
        q_start = date(current_date.year, 3 * ((current_date.month - 1) // 3) + 1, 1)
        
        # Month calculation
        m_start = date(current_date.year, current_date.month, 1)
        
        # Week calculation
        week_start = date.fromordinal(current_date.toordinal() - current_date.weekday())
        
        return {
            "date_key": int(current_date.strftime("%Y%m%d")),
            "full_date": current_date,
            "year": current_date.year,
            "quarter": (current_date.month - 1) // 3 + 1,
            "quarter_name": f"Q{(current_date.month - 1) // 3 + 1} {current_date.year}",
            "quarter_start_date": q_start,
            "quarter_end_date": q_start + relativedelta(months=3, days=-1),
            "month": current_date.month,
            "month_name": current_date.strftime("%B"),
            "month_name_short": current_date.strftime("%b"),
            "month_year": current_date.strftime("%B %Y"),
            "month_start_date": m_start,
            "month_end_date": m_start + relativedelta(months=1, days=-1),
            "week_of_year": current_date.isocalendar()[1],
            "week_of_month": (current_date.day - 1) // 7 + 1,
            "week_start_date": week_start,
            "week_end_date": week_start + timedelta(days=6),
            "day_of_month": current_date.day,
            "day_of_year": current_date.timetuple().tm_yday,
            "day_of_week": current_date.weekday() + 1,  # 1=Mon, 7=Sun
            "day_name": current_date.strftime("%A"),
            "day_name_short": current_date.strftime("%a"),
            "is_weekend": current_date.weekday() >= 5,  # 5=Sat, 6=Sun
            "is_weekday": current_date.weekday() < 5,
            "is_month_start": current_date.day == 1,
            "is_month_end": (current_date + timedelta(days=1)).month != current_date.month,
            "is_quarter_start": current_date == q_start,
            "is_quarter_end": current_date == (q_start + relativedelta(months=3, days=-1)),
            "is_year_start": current_date.month == 1 and current_date.day == 1,
            "is_year_end": current_date.month == 12 and current_date.day == 31,
        }
    
    def transform(self, 
                  records: Iterable[Dict[str, Any]], 
                  **kwargs) -> TransformResult[Dict[str, Any]]:
        """
        Ghi đè phương thức transform vì logic hoàn toàn khác.
        """
        started = datetime.now(timezone.utc)
        records_in = 0
        min_date: Optional[date] = None
        max_date: Optional[date] = None

        # Step 1: Find time range from booking data
        records_list = list(records)
        for rec in records_list:
            records_in += 1
            try:
                create_dt = to_datetime(rec.get("create_date")).date()
                checkout_dt = to_datetime(rec.get("check_out_date")).date()

                if min_date is None or create_dt < min_date:
                    min_date = create_dt
                if max_date is None or checkout_dt > max_date:
                    max_date = checkout_dt
            except Exception:
                continue

        if min_date is None or max_date is None:
            return TransformResult(
                source=self.source,
                branch_id=None,
                records_in=records_in,
                records_out=0,
                status="error",
                errors=["No valid dates found in source data."],
                started_at=started,
                finished_at=datetime.now(timezone.utc),
            )
        
        # Step 2: Generate date records for the range
        generated_dates: List[Dict[str, Any]] = []
        current_date = min_date
        while current_date <= max_date:
            generated_dates.append(self._create_date_record(current_date))
            current_date += timedelta(days=1)

        # Step 3: Dedup and validate
        deduped_dates = self._dedup(generated_dates)
        validated_dates, errors = self._validate(deduped_dates)

        finished = datetime.now(timezone.utc)
        return TransformResult(
            source=self.source,
            branch_id=None,
            records_in=records_in,
            records_out=len(validated_dates),
            errors=errors,
            data=validated_dates,
            status="success" if not errors else "partial",
            started_at=started,
            finished_at=finished,
        )
    
    def transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Not used - we override transform() method instead"""
        raise NotImplementedError("Use transform() method directly")