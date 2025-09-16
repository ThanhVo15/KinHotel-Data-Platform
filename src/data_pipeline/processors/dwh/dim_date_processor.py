# src/data_pipeline/processors/dwh/dim_date_processor.py
import pandas as pd
from pathlib import Path

from ..dwh_processor import DWHModelProcessor
from ..abstract_processor import ProcessingResult
from ..utils.date_utils import create_dim_date

class DimDateProcessor(DWHModelProcessor):
    # Xóa dataset_name khỏi __init__
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__("DimDateProcessor", execution_historical_dir, dwh_dir)

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data.")

        self.logger.info("Creating DimDate...")
        min_date = df['create_datetime'].dropna().min().date()
        max_date = df['check_out_datetime'].dropna().max().date()
        
        if pd.isna(min_date) or pd.isna(max_date):
            return ProcessingResult(name=self.name, status="error", error="Could not determine a valid date range.")

        dim_date = create_dim_date(min_date, max_date)
        
        output_path = self.dwh_dir / "dim_date" / "dim_date.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_date.to_parquet(output_path, index=False)
        self.logger.info(f"Saved DimDate to {output_path}")

        return ProcessingResult(name=self.name, input_records=len(df), output_records=len(dim_date), output_tables=[str(output_path)])