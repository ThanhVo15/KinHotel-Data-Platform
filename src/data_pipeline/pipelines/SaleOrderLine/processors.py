import pandas as pd
from pathlib import Path

from src.data_pipeline.core.dwh_processor import AbstractProcessor
from src.data_pipeline.core.abstract_processor import ProcessingResult
from src.data_pipeline.schemas.odoo_schemas import SaleOrderLine

class FactSaleOrderProcessor(AbstractProcessor):
    def __init__(self, dwh_dir: Path): 
        super().__init__("FactSaleOrderProcessor")
        self.dwh_dir = dwh_dir
        self.output_path = self.dwh_dir / "fact_sale_order" / "fact_sale_order.parquet"

    def process(self, staging_df: pd.DataFrame) -> ProcessingResult:
        if staging_df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data for FactSaleOrder.")

        self.logger.info("Creating/Overwriting final FactSaleOrder table...")
        fact_df = staging_df.copy()
        
        final_columns = list(SaleOrderLine.model_fields.keys())
        final_columns.extend(['extracted_at'])

        existing_cols = [col for col in final_columns if col in fact_df.columns]
        final_df = fact_df[existing_cols]

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(self.output_path, index=False)
        self.logger.info(f"Saved/Overwrote FactSaleOrder to {self.output_path}")

        return ProcessingResult(
            name=self.name, 
            input_records=len(fact_df), 
            output_records=len(final_df), 
            output_tables=[str(self.output_path)]
        )