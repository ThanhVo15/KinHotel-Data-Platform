# file: src/data_pipeline/schemas/pms_schemas.py

from typing import List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from datetime import date, datetime


def false_to_none(value: Any) -> Any:
    """Một validator để chuyển đổi giá trị boolean False thành None."""
    if isinstance(value, bool) and not value:
        return None
    return value

# ====== FACT BOOKING ========
class PricelistModel(BaseModel):
    id: int
    name: str

class RoomStatusModel(BaseModel):
    is_clean: bool
    is_occupied: bool

class SurveyModel(BaseModel):
    is_checkin: bool
    is_checkout: bool

class LabelModel(BaseModel):
    name: str
    color: str

class FactBooking(BaseModel):
    # --- ID & Sequence ---
    booking_line_id: int
    booking_line_sequence_id: str
    booking_id: int
    booking_sequence_id: str
    
    # --- Dates & Times ---
    check_in_date: date
    check_out_date: date
    create_date: datetime
    check_in: datetime 
    check_out: datetime 
    actual_check_in: Optional[datetime] = None
    actual_check_out: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None 

    # --- Room Info ---
    room_id: Optional[int] = None
    room_name: Optional[str] = None
    room_type_id: int
    
    # --- Price & Financials ---
    price: float
    subtotal_price: float
    total_price: float
    paid_amount: float
    remain_amount: float
    balance: float
    pricelist: Optional[PricelistModel] = None

    # --- Guest & Occupancy ---
    adult: int
    child: int
    partner_id: int
    booking_line_guest_ids: List[int]

    # --- Booking Details ---
    booking_days: int
    status: str
    note: Optional[str] = None
    is_foc: bool
    reason_approve_by: Optional[str] = None
    foc_level: Optional[str] = None

    # --- Cancellation ---
    cancel_price: float
    cancel_reason: Optional[str] = None
    
    # --- Marketing & Source ---
    source_id: int
    medium_id: int
    campaign_id: Optional[int] = None
    hotel_travel_agency_id: int

    # --- CMS & Grouping ---
    cms_booking_id: Optional[str] = None
    cms_ota_id: Optional[str] = None
    cms_booking_source: Optional[str] = None 
    group_id: int
    group_master_name: Optional[str] = None 
    group_master: Optional[str] = None

    # --- Nested Objects & Other ---
    room_status: Optional[RoomStatusModel] = None
    surveys: SurveyModel
    labels: List[LabelModel] = []
    sale_order_id: Optional[str] = None #sale_order_name
    partner_identification: Optional[str] = None
    
    _validate_false_to_none = validator(
        "cancelled_at",
        "room_id",
        "room_name",
        "note",
        "reason_approve_by",
        "foc_level",
        "cancel_reason",
        "cms_booking_id",
        "cms_ota_id",
        "cms_booking_source",
        "partner_identification",
        pre=True, 
        allow_reuse=True
    )(false_to_none)
    
    class Config:
        extra = 'ignore'

# ====== DIM DATE ========
class DimDate(BaseModel):
    """
    Standard Date Dimension Schema - Industry Best Practice
    Covers all common date attributes for reporting & analytics
    """
    # === PRIMARY KEY ===
    date_key: int  # YYYYMMDD format: 20240825
    
    # === FULL DATE ===
    full_date: date  # 2024-08-25
    
    # === YEAR ATTRIBUTES ===
    year: int  # 2024
    
    # === QUARTER ATTRIBUTES ===
    quarter: int  # 3
    quarter_name: str  # "Q3 2024"
    quarter_start_date: date  # 2024-07-01
    quarter_end_date: date  # 2024-09-30
    
    # === MONTH ATTRIBUTES ===
    month: int  # 8
    month_name: str  # "August"
    month_name_short: str  # "Aug"
    month_year: str  # "August 2024"
    month_start_date: date  # 2024-08-01
    month_end_date: date  # 2024-08-31
    
    # === WEEK ATTRIBUTES ===
    week_of_year: int  # 34
    week_of_month: int  # 4
    week_start_date: date  # Monday of this week
    week_end_date: date  # Sunday of this week
    
    # === DAY ATTRIBUTES ===
    day_of_month: int  # 25
    day_of_year: int  # 238
    day_of_week: int  # 7 (1=Mon, 7=Sun)
    day_name: str  # "Sunday"
    day_name_short: str  # "Sun"
    
    # === BUSINESS ATTRIBUTES ===
    is_weekend: bool  # True
    is_weekday: bool  # False
    is_holiday: bool = False  # Custom holidays
    holiday_name: Optional[str] = None  # "Christmas"
    
    # === PERIOD FLAGS ===
    is_month_start: bool  # False
    is_month_end: bool  # False
    is_quarter_start: bool  # False
    is_quarter_end: bool  # False
    is_year_start: bool  # False
    is_year_end: bool  # False
    
    # === RELATIVE DATE CALCULATIONS ===
    days_from_today: Optional[int] = None
    
    class Config:
        extra = 'ignore'

# ====== DIM DATE ========
class DimMajorMarket(BaseModel):
    """
    Standard Major Market Dimension Schema - Industry Best Practice
    Covers all common date attributes for reporting & analytics
    """
    price_list_id: str
    price_list_name: str
    name: str
    price: float

    class Config:
        extra = 'ignore'

# # ====== DIM BRANCH ========
# class DimBranch(BaseModel):
#     """Schema cho bang Dim_Branch"""
#     branch_id: int
#     branch_name: str
#     address: str
#     phone: str
#     total_rooms: int

# # ====== DIM MEDIUM ========
# class DimMedium(BaseModel):
#     """Schema cho bang Dim_Medium"""
#     medium_id: int
#     medium_name: str

# # ====== DIM SOURCE ========
# class DimBranch(BaseModel):
#     """Schema cho bang Dim_Source"""
#     source_id: int
#     source_name: str

# # ====== DIM CAMPAIGN ========
# class DimBranch(BaseModel):
#     """Schema cho bang Dim_Source"""
#     campaign_id: int
#     campaign_name: str

# ====== DIM CAMPAIGN ========
# class DimTravelAgency(BaseModel):
#     """Schema cho bang DimTravelAgency"""
#     travel_agency_id: int
#     name: str
#     invoice_name: str
#     external_id: str
#     notes: str
#     is_travel_agency: bool
#     is_company: bool
#     is_system: bool
#     source_id: int
#     medium_id: int
#     coutry_id: int
#     vat: 

    

    

