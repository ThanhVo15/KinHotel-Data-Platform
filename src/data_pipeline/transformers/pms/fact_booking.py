from __future__ import annotations
from typing import Dict, Any, Optional, List
import logging 

from src.data_pipeline.transformers.abstract_transformer import (
    AbstractTransformer, SCDConfig
)
from src.data_pipeline.schemas.pms_schemas import FactBooking

_NUMERIC_FIELDS = (
    "price", "subtotal_price", "total_price", "paid_amount", "remain_amount", "balance"
)

class FactBookingTransformer(AbstractTransformer[Dict[str, Any], Dict[str, Any]]):
    """
    Transformer cho PMS Bookings:
      - Chuẩn hoá và làm sạch dữ liệu đầu vào.
      - Validate theo FactBooking (Pydantic).
      - Dedup theo booking_line_sequence_id.
      - Enrich SCD Type-2 để theo dõi lịch sử thay đổi.
    """

    def __init__(self) -> None:
        track_fields: List[str] = [
            "booking_sequence_id", "status", "check_in_date", "check_out_date",
            "check_in", "check_out", "actual_check_in", "actual_check_out",
            "cancelled_at", "room_id", "room_name", "room_type_id",
            "price", "subtotal_price", "total_price", "paid_amount",
            "remain_amount", "balance", "pricelist", "adult", "child",
            "partner_id", "booking_line_guest_ids", "booking_days", "note",
            "is_foc", "cancel_reason", "source_id", "medium_id",
            "campaign_id", "hotel_travel_agency_id", "cms_booking_id",
            "group_id", "room_status", "surveys", "labels"
        ]

        super().__init__(
            name="FactBookingTransformer",
            output_schema=FactBooking,
            dedup_keys=("booking_line_sequence_id",),
            source="PMS:bookings",
            scd_config=SCDConfig(
                natural_key_fields=("booking_line_sequence_id",),
                track_fields=track_fields,
                effective_time_source="window_end",
            ),
            on_validation_error="drop", 
        )

    def transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Làm sạch và chuẩn hoá dữ liệu thô để Pydantic có thể parse một cách an toàn.
        """
        r = dict(record)

        # 1) Xử lý các giá trị "falsy" cho các trường datetime
        for dt_field in ("actual_check_in", "actual_check_out", "cancelled_at"):
            if r.get(dt_field) == "":
                r[dt_field] = None

        # 2) Ép kiểu numeric và ghi lại cảnh báo nếu thất bại
        for k in _NUMERIC_FIELDS:
            if k in r and r[k] is not None and r[k] != '':
                try:
                    r[k] = float(r[k])
                except (ValueError, TypeError):
                    self.logger.warning(
                        f"Could not convert value '{r[k]}' to float for field '{k}' "
                        f"in record with booking_line_id: {r.get('booking_line_id')}. "
                        "Leaving as is for Pydantic to validate."
                    )
        
        # 3) Chuẩn hóa booking_line_guest_ids
        blg = r.get("booking_line_guest_ids")
        if blg is not None and isinstance(blg, list):
            try:
                r["booking_line_guest_ids"] = [int(x) for x in blg if x is not None]
            except (ValueError, TypeError):
                self.logger.warning(
                    f"Could not convert all items in booking_line_guest_ids '{blg}' to int "
                    f"for record with booking_line_id: {r.get('booking_line_id')}. "
                    "Leaving as is for Pydantic to validate."
                )

        return r