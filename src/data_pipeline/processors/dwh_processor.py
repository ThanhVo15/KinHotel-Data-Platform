import pandas as pd
from pathlib import Path
from .abstract_processor import AbstractProcessor

class DWHModelProcessor(AbstractProcessor):
    def __init__(self, name: str, execution_historical_dir: Path, dwh_dir: Path):
        super().__init__(name)
        self.execution_historical_dir = execution_historical_dir
        self.dwh_dir = dwh_dir
        self.dwh_dir.mkdir(parents=True, exist_ok=True)

    def _read_current_data(self) -> pd.DataFrame:
        self.logger.info(f"Reading all historical files from {self.execution_historical_dir}")
        search_pattern = f"**/*_history.parquet"
        historical_files = list(self.execution_historical_dir.glob(search_pattern))
        
        if not historical_files:
            self.logger.warning("No historical files found. Returning empty DataFrame.")
            return pd.DataFrame()
            
        self.logger.info(f"Found {len(historical_files)} historical files to combine.")
        df_list = [pd.read_parquet(f) for f in historical_files]
        df = pd.concat(df_list, ignore_index=True)
        
        datetime_cols = ['create_datetime', 'check_in_datetime', 'check_out_datetime']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        if 'is_current' not in df.columns:
            self.logger.error("Column 'is_current' not found in combined historical data!")
            return pd.DataFrame()

        current_df = df[df['is_current']].copy()
        self.logger.info(f"Combined data has {len(current_df)} current records.")
        return current_df