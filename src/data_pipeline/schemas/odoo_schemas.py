# src/data_pipeline/schemas/odoo_schemas.py
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class SaleOrderLine(BaseModel):
    """
    Schema cho dữ liệu Sale Order Line từ Odoo sau khi đã xử lý.
    Đây là bảng Fact chính.
    """
    # Order Info
    branch_id: Optional[int] = None
    sale_order_id: int
    order_reference: Optional[str] = None
    folio_code: Optional[str] = None
    order_creation_date: datetime
    order_date: datetime
    invoice_status: Optional[str] = None
    rental_start_date: Optional[datetime] = None
    rental_return_date: Optional[datetime] = None
    duration_days: Optional[float] = None
    rental_status: Optional[str] = None
    order_status: Optional[str] = None

    # Line Info
    order_line_id: int
    line_description: Optional[str] = None
    product_id: Optional[int] = None
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    discount_percent: Optional[float] = None
    price_subtotal: Optional[float] = None
    price_total: Optional[float] = None
    total_tax: Optional[float] = None

    # Product Info
    product_is_room_type: Optional[bool] = None
    product_is_hotel_service: Optional[bool] = None

    class Config:
        extra = 'ignore'