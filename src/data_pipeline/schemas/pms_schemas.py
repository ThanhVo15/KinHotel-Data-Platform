# src/data_pipeline/schemas/pms_schemas.py
from typing import List, Optional, Any
from pydantic import BaseModel, Field
from datetime import date, datetime

class FactBooking(BaseModel):
    """
    Schema này ánh xạ trực tiếp đến các cột phẳng (flattened) mà Parser tạo ra.
    Đây là nguồn chân lý duy nhất cho cấu trúc bảng historical.
    """
    # --- ID Keys ---
    booking_line_id: int
    booking_id: int
    branch_id: int
    booking_line_sequence_id: Optional[str] = None
    booking_sequence_id: Optional[str] = None
    
    # --- Dates & Times ---
    create_datetime: datetime
    check_in_datetime: datetime 
    check_out_datetime: datetime 
    actual_check_in_datetime: Optional[datetime] = None
    actual_check_out_datetime: Optional[datetime] = None
    cancelled_at_datetime: Optional[datetime] = None 

    # --- Room Info ---
    room_id: Optional[int] = None
    room_type_id: int
    
    # --- Price & Financials ---
    price: Optional[float] = None
    subtotal_price: Optional[float] = None
    total_price: Optional[float] = None
    paid_amount: Optional[float] = None
    remain_amount: Optional[float] = None
    balance: Optional[float] = None
    
    pricelist_id: Optional[int] = None
    pricelist_name: Optional[str] = None

    # --- Guest & Occupancy ---
    num_adult: Optional[float] = None
    num_child: Optional[float] = None
    customer_id: Optional[int] = None
    booking_line_guest_ids: Optional[str] = None

    # --- Booking Details ---
    booking_days: Optional[float] = None
    status: Optional[str] = None
    note: Optional[str] = None
    is_foc: Optional[bool] = None
    reason_approve_by: Optional[str] = None
    foc_level: Optional[Any] = None

    # --- Cancellation ---
    cancel_price: Optional[float] = None
    cancel_reason: Optional[str] = None
    
    # --- Marketing & Source ---
    source_id: Optional[int] = None
    medium_id: Optional[int] = None
    campaign_id: Optional[int] = None
    hotel_travel_agency_id: Optional[int] = None

    # --- CMS & Grouping ---
    cms_booking_id: Optional[str] = None
    cms_ota_id: Optional[str] = None
    cms_booking_source: Optional[str] = None 
    group_id: Optional[int] = None
    group_master_name: Optional[str] = None 
    group_master: Optional[str] = None

    # --- Other Flat Columns ---
    room_is_clean: Optional[bool] = None
    room_is_occupied: Optional[bool] = None
    survey_is_checkin: Optional[bool] = None
    survey_is_checkout: Optional[bool] = None
    
    labels: Optional[str] = None
    sale_order_id: Optional[str] = None
    partner_identification: Optional[str] = None
    
    class Config:
        extra = 'ignore'

class DimDate(BaseModel):
    date_key: int
    full_date: date
    year: int
    quarter: int
    quarter_name: str
    month: int
    month_name: str
    month_name_short: str
    month_year: str
    week_of_year: int
    day_of_month: int
    day_of_year: int
    day_of_week: int
    day_name: str
    day_name_short: str
    is_weekend: bool
    is_weekday: bool
    is_month_start: bool
    is_month_end: bool
    is_quarter_start: bool
    is_quarter_end: bool
    is_year_start: bool
    is_year_end: bool

class DimMajorMarket(BaseModel):
    price_list_id: str
    price_list_name: str
    name: str
    price: float

class Branch(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    phone_contact: Optional[str] = None
    location: Optional[str] = None

class TravelAgency(BaseModel):
    id: int
    name: str
    invoice_name: Optional[str] = None
    external_id: Optional[str] = None
    notes: Optional[str] = None
    is_travel_agency: Optional[bool] = False
    is_company: Optional[bool] = False
    is_system: Optional[bool] = False
    source_name: Optional[str] = None
    medium_name: Optional[str] = None
    country_name: Optional[str] = None
    vat: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    commission_rate: Optional[float] = 0.0
    is_commission_deducted: Optional[bool] = False
    source_id: Optional[int] = None
    medium_id: Optional[int] = None
    country_id: Optional[int] = None

class Country(BaseModel):
    id: int
    name: str
    iso_code: Optional[str] = None

class UtmMedium(BaseModel):
    id: int
    name: str

class UtmSource(BaseModel):
    id: int
    name: str

class Customer(BaseModel):
    id: int
    name: str
    dob: Optional[date] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    identification: Optional[str] = None
    company_type: Optional[str] = None
    comment: Optional[str] = None
    contact_address_complete: Optional[str] = None
    street: Optional[str] = None
    attachments: Optional[str] = None
    state_id: Optional[int] = None
    state_name: Optional[str] = None
    country_id: Optional[int] = None
    country_name: Optional[str] = None
    vat: Optional[str] = None