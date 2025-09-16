# src/data_pipeline/pipelines/booking/processors.py
import pandas as pd
from pathlib import Path

# --- Import các thành phần từ core và utils ---
from ...core.dwh_processor import DWHModelProcessor
from ...core.abstract_processor import ProcessingResult
from ....utils.date_utils import create_dim_date
from ...schemas import pms_schemas

class FactBookingProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__(
            "FactBookingProcessor",
            execution_historical_dir,
            dwh_dir,
            dataset_name="booking"
        )

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data for FactBooking.")

        self.logger.info("Creating final FactBooking table...")
        fact_df = df.copy()
        
        # Chuyển đổi datetime sang date để join với DimDate nếu cần
        fact_df['check_in_date'] = fact_df['check_in_datetime'].dt.date
        fact_df['check_out_date'] = fact_df['check_out_datetime'].dt.date

        # Lấy danh sách cột từ Pydantic model để đảm bảo schema nhất quán
        final_columns = list(pms_schemas.FactBooking.model_fields.keys())
        
        # Thêm các cột mới đã tạo
        final_columns.extend(['check_in_date', 'check_out_date'])

        # Chỉ giữ lại các cột có trong DataFrame để tránh lỗi và đảm bảo output đúng cấu trúc
        existing_cols = [col for col in final_columns if col in fact_df.columns]
        final_df = fact_df[existing_cols]

        output_path = self.dwh_dir / "fact_booking" / "fact_booking.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path, index=False)
        self.logger.info(f"Saved FactBooking to {output_path}")

        return ProcessingResult(
            name=self.name, 
            input_records=len(df), 
            output_records=len(final_df), 
            output_tables=[str(output_path)]
        )

class DimDateProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__(
            "DimDateProcessor",
            execution_historical_dir,
            dwh_dir,
            dataset_name="booking" # Chỉ rõ dataset là "booking"
        )

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data for DimDate.")

        self.logger.info("Creating DimDate...")
        
        # Lấy ngày nhỏ nhất và lớn nhất từ dữ liệu booking
        min_date = df['create_datetime'].dropna().min().date()
        max_date = df['check_out_datetime'].dropna().max().date()
        
        if pd.isna(min_date) or pd.isna(max_date):
            return ProcessingResult(name=self.name, status="error", error="Could not determine a valid date range to build DimDate.")

        dim_date = create_dim_date(min_date, max_date)
        
        output_path = self.dwh_dir / "dim_date" / "dim_date.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_date.to_parquet(output_path, index=False)
        self.logger.info(f"Saved DimDate with {len(dim_date)} records to {output_path}")

        return ProcessingResult(name=self.name, input_records=len(df), output_records=len(dim_date), output_tables=[str(output_path)])

class DimMarketProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__(
            "DimMarketProcessor",
            execution_historical_dir,
            dwh_dir,
            dataset_name="booking" # DimMarket cũng được tạo từ dữ liệu booking
        )

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data for DimMarket.")

        self.logger.info("Creating DimMajorMarket...")
        if 'pricelist_id' not in df.columns or 'pricelist_name' not in df.columns:
            return ProcessingResult(name=self.name, status="skipped", error="Missing pricelist_id or pricelist_name columns.")
        
        dim_df = df[['pricelist_id', 'pricelist_name']].dropna(subset=['pricelist_id']).copy()
        
        # Đổi tên cột để khớp với schema DWH
        dim_df.rename(columns={'pricelist_id': 'price_list_id', 'pricelist_name': 'name'}, inplace=True)
        
        dim_df['price_list_name'] = dim_df['name']
        
        # Chuẩn hóa kiểu dữ liệu
        dim_df['price_list_id'] = dim_df['price_list_id'].astype('Int64').astype(str)
        
        # Loại bỏ trùng lặp và sắp xếp
        dim_df = dim_df.drop_duplicates(subset=['price_list_id']).sort_values('price_list_id').reset_index(drop=True)
        
        final_df = dim_df[['price_list_id', 'price_list_name', 'name']]
        
        output_path = self.dwh_dir / "dim_major_market" / "dim_major_market.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path, index=False)
        self.logger.info(f"Saved DimMajorMarket to {output_path}")

        return ProcessingResult(
            name=self.name, 
            input_records=len(df), 
            output_records=len(final_df), 
            output_tables=[str(output_path)]
        )