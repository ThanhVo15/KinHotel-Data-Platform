import pandas as pd
import logging
import re
import json
from typing import Dict, Any, Type, List
from pydantic import BaseModel, ValidationError

from src.data_pipeline.core.abstract_processor import AbstractProcessor

logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTION - ĐẶT Ở ĐÂY
# ============================================================================
def universal_cleaner(record: Dict[str, Any], schema: Type[BaseModel]) -> Dict[str, Any]:
    """
    Hàm làm sạch toàn diện, có khả năng:
    - Tự động chuyển đổi list/dict thành chuỗi JSON cho các trường string.
    - Xử lý các khóa ngoại dạng list [id, name].
    """
    cleaned_record = {}
    for field_name, field_info in schema.model_fields.items():
        raw_value = record.get(field_name)
        expected_type_str = str(field_info.annotation)

        # --- LOGIC MỚI: TỰ ĐỘNG CHUYỂN LIST/DICT SANG JSON STRING ---
        if 'str' in expected_type_str and isinstance(raw_value, (list, dict)):
            try:
                # Chuyển list hoặc dict thành một chuỗi JSON
                raw_value = json.dumps(raw_value, ensure_ascii=False)
            except TypeError:
                # Nếu có lỗi, chuyển thành chuỗi string an toàn
                raw_value = str(raw_value)
        # ---------------------------------------------------------

        if field_name.endswith('_name') and raw_value is None:
            parent_key = field_name.replace('_name', '_id')
            if parent_key in record and isinstance(record[parent_key], dict):
                raw_value = record[parent_key].get('name')

        if field_name.endswith('_id') and isinstance(raw_value, list) and len(raw_value) > 0:
            if isinstance(raw_value[0], (int, float)):
                raw_value = raw_value[0]
        
        if raw_value is False or raw_value == "False":
            raw_value = None
        
        if 'date' in expected_type_str and isinstance(raw_value, str):
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', raw_value):
                raw_value = None
        
        if field_info.is_required() and (raw_value is None or raw_value == ''):
            if 'str' in expected_type_str: raw_value = 'N/A'
            elif 'int' in expected_type_str: raw_value = -1
        
        cleaned_record[field_name] = raw_value
        
    return cleaned_record

# ============================================================================
# MAIN CLASS - SỬ DỤNG HELPER FUNCTION BÊN TRÊN
# ============================================================================
class GenericTransformer(AbstractProcessor):
    def __init__(self, dataset_name: str, schema: Type[BaseModel]):
        super().__init__(f"GenericTransformer-{schema.__name__}")
        self.dataset_name = dataset_name
        self.schema = schema

    def process(self, raw_data: List[Dict[str, Any]], **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Luồng xử lý: Raw Record -> Clean -> Validate -> Phân loại (Clean/Quarantine)
        """
        self.logger.info(f"Bắt đầu transform {len(raw_data)} records cho '{self.dataset_name}'...")
        clean_records, quarantine_records = [], []
        
        for record in raw_data:
            try:
                # Bước 1: Làm sạch dữ liệu thô
                cleaned_record = universal_cleaner(record, self.schema)
                
                # Bước 2: Validate với Pydantic Schema
                validated = self.schema(**cleaned_record)
                
                # Bước 3: Chuyển đổi Pydantic model thành dict để lưu
                final_record = validated.model_dump()
                
                # Thêm các metadata (branch_id, extracted_at,...)
                final_record.update(kwargs)
                clean_records.append(final_record)
                
            except ValidationError as e:
                # Nếu validate thất bại, đưa vào khu vực kiểm dịch (Quarantine)
                quarantine_record = record.copy()
                quarantine_record['validation_error'] = str(e)
                quarantine_record.update(kwargs)
                quarantine_records.append(quarantine_record)
        
        dfs = {
            "clean": pd.DataFrame(clean_records), 
            "quarantine": pd.DataFrame(quarantine_records)
        }
        self.logger.info(f"Transform hoàn tất: {len(dfs['clean'])} records hợp lệ, {len(dfs['quarantine'])} records bị kiểm dịch.")
        return dfs