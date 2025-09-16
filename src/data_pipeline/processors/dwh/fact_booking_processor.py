# src/data_pipeline/processors/dwh/fact_booking_processor.py
import pandas as pd
from pathlib import Path
import json

from ..dwh_processor import DWHModelProcessor
from ..abstract_processor import ProcessingResult
from ...schemas import pms_schemas

class FactBookingProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__("FactBookingProcessor", execution_historical_dir, dwh_dir)

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data.")

        self.logger.info("Creating final FactBooking table...")
        fact_df = df.copy()
        
        fact_df['check_in_date'] = fact_df['check_in_datetime'].dt.date
        fact_df['check_out_date'] = fact_df['check_out_datetime'].dt.date

        # Đảm bảo schema cuối cùng nhất quán
        final_columns = list(pms_schemas.FactBooking.model_fields.keys())
        
        # Thêm các cột mới đã tạo
        final_columns.extend(['check_in_date', 'check_out_date'])

        # Chỉ giữ lại các cột có trong DataFrame để tránh lỗi
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