import pandas as pd
from pathlib import Path
from datetime import date, datetime, timezone
from typing import List, Dict, Optional

from src.data_pipeline.core.abstract_processor import AbstractProcessor, ProcessingResult

class HistoricalProcessor(AbstractProcessor):
    def __init__(self, 
                 execution_date: date, 
                 branch_id: int, 
                 staging_dir: str, 
                 historical_dir: str, 
                 dataset_name: str,
                 primary_key: str,
                 columns_to_compare: List[str],
                 target_schema: Dict[str, str],
                 source_system: str = "pms"):
        
        super().__init__(f"HistoricalProcessor-{source_system}-{dataset_name}-Branch-{branch_id}")
        self.execution_date = execution_date
        self.branch_id = branch_id
        self.staging_dir = Path(staging_dir)
        self.historical_dir = Path(historical_dir)
        self.source_system = source_system
        self.dataset_name = dataset_name
        self.now_utc = datetime.now(timezone.utc)
        self.primary_key = primary_key
        self.columns_to_compare = columns_to_compare
        self.target_schema = target_schema

    def _find_latest_historical_file(self) -> Optional[Path]:
        self.logger.info("Finding latest historical file...")
        search_pattern = f"**/branch={self.branch_id}/{self.dataset_name}_history.parquet"
        files = list(self.historical_dir.glob(search_pattern))
        if not files:
            self.logger.warning("No historical files found for this branch.")
            return None
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        self.logger.info(f"Found latest historical file: {latest_file}")
        return latest_file

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, dtype in self.target_schema.items():
            if col not in df.columns:
                df[col] = None # Thêm cột nếu thiếu
            
            try:
                if "datetime" in dtype:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize('UTC', ambiguous='NaT')
                    else:
                        df[col] = df[col].dt.tz_convert('UTC')
                elif "Int" in dtype:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                elif "boolean" in dtype:
                    df[col] = df[col].astype(dtype, errors='ignore')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                self.logger.warning(f"Could not convert column '{col}' to '{dtype}': {e}")

        return df[list(self.target_schema.keys())]

    def _find_staging_file(self) -> Path:
        month_str = self.execution_date.strftime("%Y%m")
        search_path = self.historical_dir.parent / "staging" / self.source_system / month_str / f"branch={self.branch_id}" / self.dataset_name
        if not search_path.exists(): raise FileNotFoundError(f"Staging directory not found: {search_path}")
        files = list(search_path.glob("*.parquet"))
        if not files: raise FileNotFoundError(f"No data files found in staging path: {search_path}")
        return max(files, key=lambda p: p.stat().st_mtime)

    def _get_latest_staging_state(self) -> pd.DataFrame:
        try:
            staging_file = self._find_staging_file()
            df = pd.read_parquet(staging_file)
            latest_df = df.sort_values(by="extracted_at", ascending=False).drop_duplicates(subset=[self.primary_key], keep="first").reset_index(drop=True)
            self.logger.info(f"Found {len(latest_df)} unique records in staging for branch {self.branch_id}.")
            return latest_df
        except FileNotFoundError:
            self.logger.warning(f"No staging file found for branch {self.branch_id}.")
            return pd.DataFrame()

    def process(self) -> ProcessingResult:
        output_filename = f"{self.dataset_name}_history.parquet"
        output_path = self.historical_dir / self.execution_date.strftime('%Y/%m/%d') / f"branch={self.branch_id}" / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        latest_staging_df = self._get_latest_staging_state()
        if latest_staging_df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No new data in staging.")
        
        prev_hist_path = self._find_latest_historical_file()

        if prev_hist_path is None:
            self.logger.warning(f"No previous historical data found. Treating as first run.")
            latest_staging_df['valid_from'] = self.now_utc
            latest_staging_df['valid_to'] = pd.NaT
            latest_staging_df['is_current'] = True
            final_df = self._enforce_schema(latest_staging_df)
            final_df.to_parquet(output_path, index=False)
            return ProcessingResult(name=self.name, input_records=len(final_df), output_records=len(final_df), new_records=len(final_df), output_tables=[str(output_path)])

        prev_hist_df = pd.read_parquet(prev_hist_path)
        
        active_hist_df = self._enforce_schema(prev_hist_df[prev_hist_df['is_current']].copy())
        latest_staging_df = self._enforce_schema(latest_staging_df.copy())
        
        merged = pd.merge(active_hist_df, latest_staging_df, on=self.primary_key, how='outer', suffixes=('_hist', '_stag'), indicator=True)
        
        def create_hash(df, cols, suffix=''):
            existing_cols = [f'{c}{suffix}' for c in cols if f'{c}{suffix}' in df.columns]
            if not existing_cols:
                return pd.Series(dtype='str')
            return df[sorted(existing_cols)].astype(str).fillna('').sum(axis=1)

        merged['hash_hist'] = create_hash(merged, self.columns_to_compare, suffix='_hist')
        merged['hash_stag'] = create_hash(merged, self.columns_to_compare, suffix='_stag')

        is_both = merged['_merge'] == 'both'
        unchanged_ids = merged.loc[is_both & (merged['hash_hist'] == merged['hash_stag']), self.primary_key]
        changed_ids = merged.loc[is_both & (merged['hash_hist'] != merged['hash_stag']), self.primary_key]
        new_ids = merged.loc[merged['_merge'] == 'right_only', self.primary_key]

        final_unchanged = active_hist_df[active_hist_df[self.primary_key].isin(unchanged_ids)].copy()
        
        expired_records = active_hist_df[active_hist_df[self.primary_key].isin(changed_ids)].copy()
        if not expired_records.empty:
            expired_records['valid_to'] = self.now_utc
            expired_records['is_current'] = False
        
        new_version_records = latest_staging_df[latest_staging_df[self.primary_key].isin(changed_ids)].copy()
        if not new_version_records.empty:
            new_version_records['valid_from'] = self.now_utc
            new_version_records['valid_to'] = pd.NaT
            new_version_records['is_current'] = True
        
        final_new = latest_staging_df[latest_staging_df[self.primary_key].isin(new_ids)].copy()
        if not final_new.empty:
            final_new['valid_from'] = self.now_utc
            final_new['valid_to'] = pd.NaT
            final_new['is_current'] = True

        non_empty_parts = [part for part in [final_unchanged, expired_records, new_version_records, final_new] if not part.empty]

        if not non_empty_parts:
            self.logger.info("No changes found. Historical data is up-to-date.")
            active_hist_df.to_parquet(output_path, index=False)
            return ProcessingResult(name=self.name, status="skipped", unchanged_records=len(active_hist_df))

        combined_df = pd.concat(non_empty_parts, ignore_index=True)
        final_df = self._enforce_schema(combined_df)
        final_df.to_parquet(output_path, index=False)
        
        return ProcessingResult(
            name=self.name, input_records=len(latest_staging_df), output_records=len(final_df), 
            new_records=len(final_new), updated_records=len(new_version_records), 
            unchanged_records=len(final_unchanged), output_tables=[str(output_path)]
        )