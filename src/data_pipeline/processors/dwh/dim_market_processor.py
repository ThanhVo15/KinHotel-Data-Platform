# src/data_pipeline/processors/dwh/dim_market_processor.py
import pandas as pd
from pathlib import Path

from ..dwh_processor import DWHModelProcessor
from ..abstract_processor import ProcessingResult

class DimMarketProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__("DimMarketProcessor", execution_historical_dir, dwh_dir)

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data.")

        self.logger.info("Creating DimMajorMarket...")
        if 'pricelist_id' not in df.columns or 'pricelist_name' not in df.columns:
            return ProcessingResult(name=self.name, status="skipped", error="Missing pricelist_id or pricelist_name columns.")
        
        cols_to_select = ['pricelist_id', 'pricelist_name']
        if 'price' in df.columns:
            cols_to_select.append('price')
        else:
            df['price'] = 0.0 

        dim_df = df[cols_to_select].dropna(subset=['pricelist_id']).copy()
        dim_df.rename(columns={'pricelist_id': 'price_list_id', 'pricelist_name': 'name'}, inplace=True)
        dim_df['price_list_name'] = dim_df['name']
        dim_df['price_list_id'] = dim_df['price_list_id'].astype('Int64').astype(str)
        dim_df = dim_df.drop_duplicates(subset=['price_list_id'])
        dim_df.sort_values('price_list_id', inplace=True)
        final_df = dim_df[['price_list_id', 'price_list_name', 'name', 'price']]
        
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