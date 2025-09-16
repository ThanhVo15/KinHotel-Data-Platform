# src/data_pipeline/pipelines/odoo/parser.py
import pandas as pd
import io
from typing import List, Dict, Any
from src.data_pipeline.core.abstract_parser import AbstractParser

class OdooSaleOrderParser(AbstractParser):
    """Parser để đọc chuỗi CSV từ Odoo và chuyển thành DataFrame sạch."""

    def parse(self, data: str, **kwargs) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()

        # Đọc chuỗi CSV bằng pandas
        df = pd.read_csv(io.StringIO(data))

        # Đổi tên cột cho thân thiện
        rename_map = {
            'branch_id/id': 'branch_id',
            '.id': 'sale_order_id',
            'name': 'order_reference',
            'create_date': 'order_creation_date',
            'order_line/.id': 'order_line_id',
            'order_line/name': 'line_description',
            'order_line/product_id/id': 'product_id',
            'order_line/product_uom_qty': 'quantity',
            'order_line/price_unit': 'unit_price',
            'order_line/discount': 'discount_percent',
            'order_line/price_subtotal': 'price_subtotal',
            'order_line/price_total': 'price_total',
            'order_line/price_tax': 'total_tax',
            'order_line/product_id/is_room_type': 'product_is_room_type',
            'order_line/product_id/is_hotel_service': 'product_is_hotel_service',
            'state': 'order_status'
        }
        df.rename(columns=rename_map, inplace=True)

        # Chuyển đổi kiểu dữ liệu
        date_cols = [
            'order_creation_date', 'order_date', 
            'rental_start_date', 'rental_return_date'
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Thêm các cột metadata
        df['extracted_at'] = pd.Timestamp.utcnow()

        return df