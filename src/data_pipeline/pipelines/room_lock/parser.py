import pandas as pd
from typing import List, Dict, Any
from src.data_pipeline.core.abstract_parser import AbstractParser

class RoomLockParser(AbstractParser):
    """
    Parser chuyên dụng để làm phẳng và chuẩn hóa dữ liệu Room Lock.
    """
    COLUMNS = [
        'id', 'reason', 'room_id', 'room_no', 'room_name', 'room_type_name',
        'start_date', 'end_date', 'original_end_date', 'create_username',
        'create_date', 'active', 'branch_id', 'extracted_at'
    ]

    def parse(self, data: List[Dict[str, Any]], **kwargs) -> pd.DataFrame:
        branch_id = kwargs.get("branch_id")
        if branch_id is None:
            raise ValueError("branch_id is required for RoomLockParser")

        flattened_records = [self._flatten_record(rec, branch_id) for rec in data]
        if not flattened_records:
            return pd.DataFrame(columns=self.COLUMNS)

        df = pd.DataFrame(flattened_records)

        # Chuẩn hóa kiểu dữ liệu
        date_cols = ['start_date', 'end_date', 'original_end_date', 'create_date']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        for col in self.COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df[self.COLUMNS]

    def _flatten_record(self, rec: Dict[str, Any], branch_id: int) -> Dict[str, Any]:
        """
        Hàm làm phẳng một record JSON thô.
        Đã thêm logic để xử lý các giá trị `False` trong các trường string.
        """
        attributes = rec.get("attributes", {}) or {}

        # --- HÀM HELPER ĐỂ CHUẨN HÓA STRING ---
        def to_safe_string(value: Any) -> str | None:
            """Chuyển đổi giá trị thành string một cách an toàn, xử lý None và False."""
            if value is None or value is False:
                return None
            return str(value)
        # ------------------------------------

        return {
            "id": rec.get("id"),
            # --- ÁP DỤNG HÀM HELPER ---
            "reason": to_safe_string(rec.get("reason")),
            "room_id": rec.get("room_id"),
            "room_no": to_safe_string(attributes.get("room_no")),
            "room_name": to_safe_string(rec.get("room_name")),
            "room_type_name": to_safe_string(rec.get("room_type_name")),
            # ---------------------------
            "start_date": rec.get("start_date"),
            "end_date": rec.get("end_date"),
            "original_end_date": rec.get("original_end_date"),
            # --- ÁP DỤNG HÀM HELPER ---
            "create_username": to_safe_string(rec.get("create_username")),
            # ---------------------------
            "create_date": rec.get("create_date"),
            "active": rec.get("active"),
            "branch_id": branch_id,
            "extracted_at": pd.Timestamp.utcnow()
        }