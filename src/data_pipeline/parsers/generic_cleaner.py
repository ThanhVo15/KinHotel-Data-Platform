import logging
import re
import json
from typing import Dict, Any, Type
from pydantic import BaseModel

logger = logging.getLogger(__name__)

def universal_cleaner(record: Dict[str, Any], schema: Type[BaseModel]) -> Dict[str, Any]:
    cleaned_record = {}
    for field_name, field_info in schema.model_fields.items():
        raw_value = record.get(field_name)

        if '_' in field_name:
            parent_key, child_key = field_name.split('_', 1)
            if parent_key in record and isinstance(record[parent_key], dict):
                raw_value = record[parent_key].get(child_key)
        
        if raw_value is False:
            raw_value = None
        
        expected_type = str(field_info.annotation)
        if 'date' in expected_type and isinstance(raw_value, str):
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', raw_value):
                raw_value = None
        
        if field_info.is_required() and (raw_value is None or raw_value == ''):
            if 'str' in expected_type: raw_value = 'N/A'
            elif 'int' in expected_type: raw_value = -1
        
        cleaned_record[field_name] = raw_value
    return cleaned_record