# src/data_pipeline/extractors/pms/booking.py
import asyncio
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
import random

import aiohttp

from ....utils.state_manager import load_last_run_timestamp, save_last_run_timestamp
from ....utils.date_params import DateWindow, DateField, ICT
from .pms_extractor import PMSExtractor
from ..abstract_extractor import ExtractionResult

logger = logging.getLogger(__name__)

COLUMNS = [
    "branch_id",
    "booking_line_id", "booking_line_sequence_id", "booking_id", "booking_sequence_id",
    "sale_order_id",
    "room_id", "room_type_id",
    "create_datetime",
    "check_in_datetime", "actual_check_in_datetime",
    "check_out_datetime", "actual_check_out_datetime",
    "cancelled_at_datetime",
    "status",
    "price", "booking_days", "paid_amount", "subtotal_price", "total_price", "balance", "remain_amount",
    "cancel_price", "cancel_reason",
    "num_adult", "num_child",
    "customer_id", "partner_identification", "booking_line_guest_ids",
    "medium_id", "source_id", "campaign_id",
    "cms_booking_id", "cms_ota_id", "cms_booking_source",
    "group_master_name", "labels", "hotel_travel_agency_id",
    "group_id", "group_master",
    "room_is_clean", "room_is_occupied", "survey_is_checkin", "survey_is_checkout",
    "pricelist_id", "pricelist_name",
    "extracted_at", "scd_valid_from", "scd_valid_to", "scd_is_current",
]

class BookingListExtractor(PMSExtractor):
    ENDPOINT = "bookings"
    logger = logging.getLogger(__name__)

    @staticmethod
    def _flatten_record(rec: Dict[str, Any],
                        branch_id: int,
                        branch_name: str,
                        extracted_at: datetime) -> Dict[str, Any]:
        room_status = rec.get("room_status") or {}
        surveys = rec.get("surveys") or {}
        pricelist = rec.get("pricelist") or {}

        flat = {
            "branch_id": branch_id,

            "booking_line_id": rec.get("booking_line_id"),
            "booking_line_sequence_id": rec.get("booking_line_sequence_id"),
            "booking_id": rec.get("booking_id"),
            "booking_sequence_id": rec.get("booking_sequence_id"),
            "sale_order_id": rec.get("sale_order_name"),

            "room_id": rec.get("room_id"),
            # "room_name": rec.get("room_name"),
            "room_type_id": rec.get("room_type_id"),
            # "room_type_name": rec.get("room_type_name"),
            # "original_room_type_name": rec.get("original_room_type_name"),
            # "display_room_type_name": rec.get("display_room_type_name"),
            # "room_no": attributes.get("room_no"),

            "create_datetime": rec.get("create_date"),
            # "check_in_date": rec.get("check_in_date"),
            # "check_out_date": rec.get("check_out_date"),
            "check_in_datetime": rec.get("check_in"),
            "actual_check_in_datetime": rec.get("actual_check_in"),
            "check_out_datetime": rec.get("check_out"),
            "actual_check_out_datetime": rec.get("actual_check_out"),
            "cancelled_at_datetime": rec.get("cancelled_at"),
            "status": rec.get("status"),

            "price": rec.get("price"),
            "booking_days": rec.get("booking_days"),
            "paid_amount": rec.get("paid_amount"),
            "subtotal_price": rec.get("subtotal_price"),
            "total_price": rec.get("total_price"),
            "balance": rec.get("balance"),
            "remain_amount": rec.get("remain_amount"),
            "cancel_price": rec.get("cancel_price"),
            "cancel_reason": rec.get("cancel_reason"),

            "num_adult": rec.get("adult"),
            "num_child": rec.get("child"),

            "customer_id": rec.get("partner_id"),
            # "customer_name": rec.get("partner_name"),
            "partner_identification": rec.get("partner_identification"),
            # "gender": rec.get("gender"),
            "booking_line_guest_ids": rec.get("booking_line_guest_ids"),

            "medium_id": rec.get("medium_id"),
            # "medium_name": rec.get("medium_name"),
            "source_id": rec.get("source_id"),
            # "source_name": rec.get("source_name"),
            "campaign_id": rec.get("campaign_id"),
            # "campaign_name": rec.get("campaign_name"),

            "cms_booking_id": rec.get("cms_booking_id"),
            "cms_ota_id": rec.get("cms_ota_id"),
            "cms_booking_source": rec.get("cms_booking_source"),

            "group_master_name": rec.get("group_master_name"),
            "labels": rec.get("labels"),
            # "hotel_travel_agency_name": rec.get("hotel_travel_agency_name"),
            "hotel_travel_agency_id": rec.get("hotel_travel_agency_id"),

            "group_id": rec.get("group_id"),
            "group_master": rec.get("group_master"),

            "room_is_clean": room_status.get("is_clean"),
            "room_is_occupied": room_status.get("is_occupied"),
            "survey_is_checkin": surveys.get("is_checkin"),
            "survey_is_checkout": surveys.get("is_checkout"),

            "pricelist_id": pricelist.get("id"),
            "pricelist_name": pricelist.get("name"),

            "extracted_at": extracted_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "scd_valid_from": extracted_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "scd_valid_to": "",
            "scd_is_current": 1,
        }
        return {col: flat.get(col) for col in COLUMNS}

    async def _perform_extraction(self, session: aiohttp.ClientSession, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        # Lấy params từ kwargs và phân trang qua hàm chung của lớp cha (đã wrap PMSClient)
        url = f"{self.client.base_url}{self.ENDPOINT}"
        base_params: Dict[str, Any] = kwargs.get("params", {})
        if "limit" not in base_params:
            base_params["limit"] = 100
        return await self._paginate(session, url, base_params, limit_default=100)

    async def extract_async(self, *, branch_id: int = 1, **kwargs) -> ExtractionResult:
        start_ts = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")

        # Resolve DateWindow (ưu tiên date_window)
        dw: Optional[DateWindow] = kwargs.pop("date_window", None)
        if dw is None:
            from datetime import timedelta
            check_in_from = kwargs.pop("check_in_from", None)
            check_in_to = kwargs.pop("check_in_to", None)
            created_from = kwargs.pop("created_date_from", None)
            created_to = kwargs.pop("created_date_to", None)

            def _parse(v):
                if v is None:
                    return None
                if isinstance(v, datetime):
                    return v
                try:
                    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                except Exception:
                    self.logger.warning(f"Unrecognized datetime format: {v}")
                    return None

            if check_in_from or check_in_to:
                s = _parse(check_in_from) or (start_ts - timedelta(days=1))
                e = _parse(check_in_to) or start_ts
                dw = DateWindow(start=s, end=e, field="check_in", tz=ICT)
            elif created_from or created_to:
                s = _parse(created_from) or (start_ts - timedelta(days=1))
                e = _parse(created_to) or start_ts
                dw = DateWindow(start=s, end=e, field="create", tz=ICT)
            else:
                raise ValueError("Missing date_window or legacy {check_in_*}/{created_date_*} params.")

        source_key = f"{self.ENDPOINT}:{dw.field}"

        try:
            session = await self._get_session(branch_id)

            params: Dict[str, Any] = {**dw.as_api_params(), **kwargs}
            if "limit" not in params:
                params["limit"] = 100

            self.logger.info(
                f"🚀 Starting extraction {source_key} for {branch_name}: "
                f"{params.get('check_in_from') or params.get('created_date_from')} → "
                f"{params.get('check_in_to') or params.get('created_date_to')} (limit={params['limit']})"
            )

            raw_records = await self._perform_extraction(session, branch_id, params=params)

            flattened = [
                self._flatten_record(r, branch_id, branch_name, extracted_at=start_ts)
                for r in raw_records
            ]

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self.logger.info(f"✅ PMS {branch_name}: Extracted {len(flattened)} records in {duration:.2f}s")

            # Lưu watermark theo UTC end window
            save_last_run_timestamp(
                source=source_key,
                branch_id=branch_id,
                timestamp=dw.end.astimezone(timezone.utc),
            )

            return ExtractionResult(
                data=flattened,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_ts,
                record_count=len(flattened),
                status="success",
                created_date_from=dw.start.astimezone(timezone.utc),
                created_date_to=dw.end.astimezone(timezone.utc),
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self.logger.error(f"❌ PMS {branch_name} failed after {duration:.2f}s: {e}")
            return ExtractionResult(
                data=None,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_ts,
                status="error",
                error=str(e),
                created_date_from=dw.start.astimezone(timezone.utc) if dw else None,
                created_date_to=dw.end.astimezone(timezone.utc) if dw else None,
            )

    async def extract_bookings_incrementally(
        self,
        branch_ids: Optional[List[int]] = None,
        lookback_days: int = 1,
        *,
        field: DateField = "check_in",
        max_concurrent: int = 5,
        **kwargs,
    ) -> Dict[int, ExtractionResult]:
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        source_key = f"{self.ENDPOINT}:{field}"
        now_utc = datetime.now(timezone.utc)
        sem = asyncio.Semaphore(max_concurrent)

        async def _one(bid: int) -> ExtractionResult:
            async with sem:
                try:
                    last_run = load_last_run_timestamp(source=source_key, branch_id=bid)
                    start_utc = last_run or (now_utc - timedelta(days=lookback_days))
                    dw = DateWindow.from_utc(start_utc, now_utc, field=field, tz=ICT)
                    self.logger.info(f"🗓️ Window for branch {bid} ({field}): {dw.start} → {dw.end} (UTC)")
                    await asyncio.sleep(random.random() * 0.8)
                    return await self.extract_async(branch_id=bid, date_window=dw, **kwargs)
                except Exception as e:
                    return ExtractionResult(
                        data=None,
                        source=f"PMS:{self.ENDPOINT}",
                        branch_id=bid,
                        branch_name=self.TOKEN_BRANCH_MAP.get(bid, f"Branch {bid}"),
                        status="error",
                        error=str(e),
                        extracted_at=now_utc,
                        created_date_from=start_utc if 'start_utc' in locals() else None,
                        created_date_to=now_utc,
                    )

        results = await asyncio.gather(*[_one(b) for b in branch_ids])
        return {r.branch_id: r for r in results}
    
    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        """
        AbstractExtractor yêu cầu extract_multi_branch; ta reuse lại hàm
        extract_bookings_incrementally để giữ đúng logic & log hiện có.
        """
        return await self.extract_bookings_incrementally(**kwargs)

