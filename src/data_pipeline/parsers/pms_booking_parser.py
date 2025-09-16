# src/data_pipeline/parsers/pms_booking_parser.py
import pandas as pd
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

from .abstract_parser import AbstractParser

class PMSBookingParser(AbstractParser):
    COLUMNS = [
        "branch_id", "booking_line_id", "booking_id", "booking_line_sequence_id", "booking_sequence_id",
        "sale_order_id", "room_id", "room_type_id", "create_datetime",
        "check_in_datetime", "actual_check_in_datetime", "check_out_datetime", "actual_check_out_datetime",
        "cancelled_at_datetime", "status", "price", "booking_days", "paid_amount", "subtotal_price",
        "total_price", "balance", "remain_amount", "cancel_price", "cancel_reason", "num_adult", "num_child",
        "customer_id", "partner_identification", "booking_line_guest_ids", "medium_id", "source_id", "campaign_id",
        "cms_booking_id", "cms_ota_id", "cms_booking_source", "group_master_name", "labels",
        "hotel_travel_agency_id", "group_id", "group_master", "room_is_clean", "room_is_occupied",
        "survey_is_checkin", "survey_is_checkout", "pricelist_id", "pricelist_name", 'note', 'is_foc',
        'reason_approve_by', 'foc_level', "extracted_at"
    ]

    def parse(self, data: List[Dict[str, Any]], **kwargs) -> pd.DataFrame:
        branch_id = kwargs.get("branch_id")
        if branch_id is None:
            raise ValueError("branch_id is required for PMSBookingParser")
            
        extracted_at = datetime.now(timezone.utc)
        
        flattened_records = [self._flatten_record(rec, branch_id, extracted_at) for rec in data]
        
        if not flattened_records:
            return pd.DataFrame(columns=self.COLUMNS)
            
        df = pd.DataFrame(flattened_records)
        
        datetime_cols = [
            'create_datetime', 'check_in_datetime', 'check_out_datetime',
            'actual_check_in_datetime', 'actual_check_out_datetime', 'cancelled_at_datetime'
        ]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        integer_cols = ['room_id', 'customer_id', 'medium_id', 'source_id', 'campaign_id', 
                        'hotel_travel_agency_id', 'group_id', 'pricelist_id']
        for col in integer_cols:
             if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        for col in self.COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df[self.COLUMNS]

    def _flatten_record(self, rec: Dict[str, Any], branch_id: int, extracted_at: datetime) -> Dict[str, Any]:
        def clean_value(value):
            if isinstance(value, bool) and not value: return None
            if value == "False": return None
            return value

        def safe_int_convert(value):
            cleaned = clean_value(value)
            if cleaned is None: return None
            try: return int(cleaned)
            except (ValueError, TypeError): return None
        
        def stringify_complex_types(value):
            cleaned = clean_value(value)
            if isinstance(cleaned, (list, dict)): return json.dumps(cleaned)
            return cleaned

        pricelist = rec.get("pricelist") or {}
        room_status = rec.get("room_status") or {}
        surveys = rec.get("surveys") or {}

        flat = {
            "branch_id": branch_id,
            "booking_line_id": rec.get("booking_line_id"),
            "booking_id": rec.get("booking_id"),
            "booking_line_sequence_id": rec.get("booking_line_sequence_id"),
            "booking_sequence_id": rec.get("booking_sequence_id"),
            "sale_order_id": clean_value(rec.get("sale_order_name")),
            "room_id": safe_int_convert(rec.get("room_id")),
            "room_type_id": rec.get("room_type_id"),
            "create_datetime": rec.get("create_date"),
            "check_in_datetime": rec.get("check_in"),
            "check_out_datetime": rec.get("check_out"),
            "actual_check_in_datetime": clean_value(rec.get("actual_check_in")),
            "actual_check_out_datetime": clean_value(rec.get("actual_check_out")),
            "cancelled_at_datetime": clean_value(rec.get("cancelled_at")),
            "status": rec.get("status"),
            "price": rec.get("price"),
            "booking_days": rec.get("booking_days"),
            "paid_amount": rec.get("paid_amount"),
            "subtotal_price": rec.get("subtotal_price"),
            "total_price": rec.get("total_price"),
            "balance": rec.get("balance"),
            "remain_amount": rec.get("remain_amount"),
            "cancel_price": rec.get("cancel_price"),
            "cancel_reason": str(clean_value(rec.get("cancel_reason")) or ''),
            "num_adult": rec.get("adult"),
            "num_child": rec.get("child"),
            "customer_id": safe_int_convert(rec.get("partner_id")),
            "partner_identification": str(clean_value(rec.get("partner_identification")) or ''),
            "booking_line_guest_ids": stringify_complex_types(rec.get("booking_line_guest_ids")),
            "medium_id": safe_int_convert(rec.get("medium_id")),
            "source_id": safe_int_convert(rec.get("source_id")),
            "campaign_id": safe_int_convert(rec.get("campaign_id")),
            "hotel_travel_agency_id": safe_int_convert(rec.get("hotel_travel_agency_id")),
            "cms_booking_id": clean_value(rec.get("cms_booking_id")),
            "cms_ota_id": clean_value(rec.get("cms_ota_id")),
            "cms_booking_source": clean_value(rec.get("cms_booking_source")),
            "group_master_name": clean_value(rec.get("group_master_name")),
            "labels": stringify_complex_types(rec.get("labels")),
            "group_id": safe_int_convert(rec.get("group_id")),
            "group_master": clean_value(rec.get("group_master")),
            "room_is_clean": room_status.get("is_clean"),
            "room_is_occupied": room_status.get("is_occupied"),
            "survey_is_checkin": surveys.get("is_checkin"),
            "survey_is_checkout": surveys.get("is_checkout"),
            "pricelist_id": safe_int_convert(pricelist.get("id")),
            "pricelist_name": pricelist.get("name"),
            "note": clean_value(rec.get("note")),
            "is_foc": rec.get("is_foc"),
            "reason_approve_by": clean_value(rec.get("reason_approve_by")),
            "foc_level": clean_value(rec.get("foc_level")),
            "extracted_at": extracted_at.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return {key: flat.get(key) for key in self.COLUMNS}