from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import date

# --- Import các class Chiến lược ---
from src.data_pipeline.extractors.pms.extraction_strategies import FullLoadStrategy, PaginatedLoadStrategy

# --- Import các class Schema ---
from src.data_pipeline.schemas.pms_schemas import Branch, TravelAgency, Customer, Country, UtmMedium, UtmSource

# --- Hàm helper để tự động tạo config ---
def generate_dwh_columns(schema: type[BaseModel], primary_key: str, key_suffix: str = "_key") -> Dict[str, str]:
    columns = {}
    for field_name in schema.model_fields.keys():
        if field_name == primary_key:
            # Đổi tên khóa chính, ví dụ: 'id' -> 'customer_key'
            clean_name = schema.__name__.lower().replace(' ', '_')
            columns[field_name] = f"{clean_name}{key_suffix}"
        else:
            columns[field_name] = field_name
    return columns

# ============================================================================
# TRUNG TÂM ĐIỀU KHIỂN - KHAI BÁO CÁC PIPELINE CẦN CHẠY
# ============================================================================
DIMENSIONS_CONFIG: List[Dict[str, Any]] = [
    {
        "name": "branches",
        "endpoint": "branches",
        "load_strategy": FullLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": Branch,
        "primary_key": "id",
    },
    {
        "name": "travel_agencies",
        "endpoint": "travel-agencies",
        "load_strategy": PaginatedLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": TravelAgency,
        "primary_key": "id",
    },
    {
        "name": "countries",
        "endpoint": "res/countries",
        "load_strategy": FullLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": Country,
        "primary_key": "id",
    },
    {
        "name": "utm_mediums",
        "endpoint": "utm/mediums",
        "load_strategy": FullLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": UtmMedium,
        "primary_key": "id",
    },
    {
        "name": "utm_sources",
        "endpoint": "utm/sources",
        "load_strategy": FullLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": UtmSource,
        "primary_key": "id",
    },
    {
        "name": "customers",
        "endpoint": "customers",
        "load_strategy": PaginatedLoadStrategy(), # <<< Chỉ định chiến lược
        "schema": Customer,
        "primary_key": "id",
    }
]

# --- Tự động điền các thông tin config còn lại ---
for dim in DIMENSIONS_CONFIG:
    dim["historical_columns"] = list(dim["schema"].model_fields.keys())
    dim["dwh_table_name"] = f"dim_{dim['name']}"
    dim["dwh_columns"] = generate_dwh_columns(dim["schema"], dim["primary_key"])