# file: src/data_pipeline/schemas/pms_schemas.py

from typing import List, Optional, Any, Union
from pydantic import BaseModel, Field
from datetime import date, datetime

# ====== BOOKING PIPELINE ========
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

class PmsBookingRecord(BaseModel):
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
    cancelled_at: Optional[datetime]

    # --- Room Info ---
    room_id: Optional[Union[int, bool]] = None
    room_name: Optional[Union[str, bool]] = None
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
    note: Optional[Union[str, bool]] = None
    is_foc: bool
    reason_approve_by: Optional[Union[str, bool]] = None
    foc_level: Optional[Union[str, bool]] = None

    # --- Cancellation ---
    cancel_price: float
    cancel_reason: Optional[Union[str, bool]] = None
    
    # --- Marketing & Source ---
    source_id: int
    medium_id: int
    campaign_id: Optional[int] = None
    hotel_travel_agency_id: int

    # --- CMS & Grouping ---
    cms_booking_id: Optional[Union[str, bool]] = None
    cms_ota_id: Optional[Union[str, bool]] = None
    cms_booking_source: Optional[Union[str, bool]] = None   
    group_id: int                                           
    group_master_name: Any                                  
    group_master: Any                                       

    # --- Nested Objects & Other ---
    room_status: Optional[RoomStatusModel] = None
    surveys: SurveyModel
    labels: List[LabelModel] = []
    sale_order_id: Any # sale_order_name
    partner_identification: Optional[Union[str, bool]] = None
    
    class Config:
        extra = 'ignore'