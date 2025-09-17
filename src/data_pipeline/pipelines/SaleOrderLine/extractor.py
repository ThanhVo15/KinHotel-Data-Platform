from src.data_pipeline.core.abstract_extractor import AbstractExtractor, ExtractionResult
from src.data_pipeline.core.clients.odoo_client import OdooClient

class OdooSaleOrderExtractor(AbstractExtractor):
    """Extractor để lấy dữ liệu Sale Order từ Odoo."""
    def __init__(self, client: OdooClient):
        super().__init__("OdooSaleOrder")
        self.client = client
        self.model = "sale.order"
        self.fields = [
            {"name":"branch_id","label":"Branch"},
            {"name":".id","label":"ID"},
            {"name":"name","label":"Order Reference"},
            {"name":"folio_code","label":"Folio Code"},
            {"name":"create_date","label":"Creation Date"},
            {"name":"order_line/.id","label":"Order Lines/ID"},
            {"name":"order_line/name","label":"Order Lines/Description"},
            {"name":"order_line/product_id","label":"Order Lines/Product"},
            {"name":"order_line/product_uom_qty","label":"Order Lines/Quantity"},
            {"name":"order_line/price_unit","label":"Order Lines/Unit Price"},
            {"name":"order_line/discount","label":"Order Lines/Discount (%)"},
            {"name":"order_line/price_subtotal","label":"Order Lines/Subtotal"},
            {"name":"order_line/price_total","label":"Order Lines/Total"},
            {"name":"order_line/tax_id","label":"Order Lines/Taxes"},
            {"name":"order_line/price_tax","label":"Order Lines/Total Tax"},
            {"name":"invoice_status","label":"Invoice Status"},
            {"name":"date_order","label":"Order Date"},
            {"name":"rental_start_date","label":"Rental Start Date"},
            {"name":"rental_return_date","label":"Rental Return Date"},
            {"name":"duration_days","label":"Duration in days"},
            {"name":"rental_status","label":"Rental Status"},
            {"name":"state","label":"Status"},
            {"name":"order_line/product_id/is_room_type","label":"Order Lines/Product/Is Room Type"},
            {"name":"order_line/product_id/is_hotel_service","label":"Order Lines/Product/Is Hotel Service"}
        ]

    async def extract_async(self, **kwargs) -> ExtractionResult:
        """Thực hiện full load."""
        csv_data = await self.client.export_csv(self.model, self.fields)
        if csv_data:
            return ExtractionResult(data=csv_data, source="Odoo:SaleOrder", record_count=csv_data.count('\n'))
        else:
            return ExtractionResult(data=None, source="Odoo:SaleOrder", status="error", error="Failed to fetch CSV data from Odoo.")

    # Không cần extract_multi_branch vì đây là full load
    async def extract_multi_branch(self, **kwargs):
        raise NotImplementedError
    
    async def close(self):
        # Client sẽ được đóng bởi main.py
        pass