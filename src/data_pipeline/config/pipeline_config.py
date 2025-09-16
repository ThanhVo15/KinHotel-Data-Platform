from typing import List, Dict, Any, Type
from pydantic import BaseModel

# --- Import các class Schema ---
from ..schemas.pms_schemas import Branch, TravelAgency, Customer, Country, UtmMedium, UtmSource

# --- Hàm helper để tự động tạo config ---
def generate_dwh_columns(schema: Type[BaseModel], primary_key: str, key_suffix: str = "_key") -> Dict[str, str]:
    """Tự động tạo mapping cột cho DWH từ Pydantic schema."""
    columns = {}
    # Lấy tên class, chuyển thành snake_case (ví dụ: TravelAgency -> travel_agency)
    clean_name = ''.join(['_' + i.lower() if i.isupper() else i for i in schema.__name__]).lstrip('_')
    
    for field_name in schema.model_fields.keys():
        if field_name == primary_key:
            # Đổi tên khóa chính, ví dụ: 'id' -> 'customer_key'
            columns[field_name] = f"{clean_name}{key_suffix}"
        else:
            columns[field_name] = field_name
    return columns

# ============================================================================
# TRUNG TÂM ĐIỀU KHIỂN - KHAI BÁO CÁC PIPELINE DIMENSIONS CẦN CHẠY
# ============================================================================
DIMENSIONS_CONFIG: List[Dict[str, Any]] = [
    {
        "name": "branches",
        "endpoint": "branches",
        "is_paginated": False, # Endpoint này không phân trang
        "is_shared_across_branches": True, # Dữ liệu dùng chung cho mọi chi nhánh
        "schema": Branch,
        "primary_key": "id",
    },
    {
        "name": "travel_agencies",
        "endpoint": "travel-agencies",
        "is_paginated": False,
        "is_shared_across_branches": True,
        "schema": TravelAgency,
        "primary_key": "id",
    },
    {
        "name": "countries",
        "endpoint": "res/countries",
        "is_paginated": False,
        "is_shared_across_branches": True,
        "schema": Country,
        "primary_key": "id",
    },
    {
        "name": "utm_mediums",
        "endpoint": "utm/mediums",
        "is_paginated": False,
        "is_shared_across_branches": True,
        "schema": UtmMedium,
        "primary_key": "id",
    },
    {
        "name": "utm_sources",
        "endpoint": "utm/sources",
        "is_paginated": False,
        "is_shared_across_branches": True,
        "schema": UtmSource,
        "primary_key": "id",
    },
    {
        "name": "customers",
        "endpoint": "customers",
        "is_paginated": True,
        "is_shared_across_branches": True,
        "base_params": {"limit": 10000},
        "schema": Customer,
        "primary_key": "id",
    }
]

# --- Tự động điền các thông tin config còn lại ---
for dim in DIMENSIONS_CONFIG:
    # Các cột cần so sánh thay đổi trong historical
    dim["historical_columns"] = list(dim["schema"].model_fields.keys())
    # Tên bảng trong DWH
    dim["dwh_table_name"] = f"dim_{dim['name']}"
    # Mapping tên cột từ staging -> dwh
    dim["dwh_columns"] = generate_dwh_columns(dim["schema"], dim["primary_key"])

# Mapping token branches
TOKEN_BRANCH_MAP = {
    1:  "KIN HOTEL DONG DU",
    2:  "KIN HOTEL THI SACH EDITION",
    3:  "KIN HOTEL THAI VAN LUNG",
    4:  "KIN WANDER TAN BINH, THE MOUNTAIN",
    5:  "KIN WANDER TAN QUY",
    6:  "KIN WANDER TAN PHONG, THE MOONAGE",
    7:  "KIN WANDER TRUNG SON",
    9:  "KIN HOTEL CENTRAL PARK",
    10: "KIN HOTEL LY TU TRONG",
}