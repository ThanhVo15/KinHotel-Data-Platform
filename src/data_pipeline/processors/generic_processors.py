import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Type
from pydantic import BaseModel, ValidationError
import json

# Giả sử các import này là đúng với cấu trúc của bạn
from .abstract_processor import AbstractProcessor, ProcessingResult
from src.data_pipeline.processors.dwh_processor import DWHModelProcessor
from src.data_pipeline.parsers.generic_cleaner import universal_cleaner

class GenericTransformer(AbstractProcessor):
    def __init__(self, dataset_name: str, schema: Type[BaseModel]):
        super().__init__(f"GenericTransformer-{schema.__name__}")
        self.dataset_name = dataset_name
        self.schema = schema

    def process(self, raw_data: List[Dict[str, Any]], **kwargs) -> Dict[str, pd.DataFrame]:
        self.logger.info(f"Bắt đầu transform {len(raw_data)} records...")
        clean_records, quarantine_records = [], []
        
        for record in raw_data:
            try:
                cleaned_record = universal_cleaner(record, self.schema)
                validated = self.schema(**cleaned_record)
                final_record = validated.model_dump()
                final_record.update(kwargs)
                clean_records.append(final_record)
            except ValidationError as e:
                quarantine_record = record.copy()
                quarantine_record['validation_error'] = str(e)
                quarantine_record.update(kwargs)
                quarantine_records.append(quarantine_record)
        
        dfs = {"clean": pd.DataFrame(clean_records), "quarantine": pd.DataFrame(quarantine_records)}
        self.logger.info(f"Transform hoàn tất: {len(dfs['clean'])} records hợp lệ, {len(dfs['quarantine'])} records bị kiểm dịch.")
        return dfs

class GenericDimProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path, dataset_name: str, dwh_table_name: str, dwh_columns: Dict[str, str], clean_schema: Type[BaseModel], primary_key: str):
        super().__init__(f"GenericDimProcessor-{dataset_name}", execution_historical_dir, dwh_dir)
        self.dataset_name, self.dwh_table_name, self.dwh_columns, self.clean_schema, self.primary_key = dataset_name, dwh_table_name, dwh_columns, clean_schema, primary_key

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty: return ProcessingResult(name=self.name, status="skipped")

        clean_df, conversion_errors = self._enforce_schema_and_collect_errors(df)
        
        final_df = clean_df.rename(columns=self.dwh_columns)
        dwh_pk = self.dwh_columns.get(self.primary_key, self.primary_key)
        final_df = final_df.drop_duplicates(subset=[dwh_pk]).reset_index(drop=True)
        
        output_path = self.dwh_dir / self.dwh_table_name / f"{self.dwh_table_name}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path, index=False)
        
        result = ProcessingResult(name=self.name, input_records=len(df), output_records=len(final_df), output_tables=[str(output_path)])
        if conversion_errors: result.error = json.dumps(conversion_errors)
        return result

    def _enforce_schema_and_collect_errors(self, df: pd.DataFrame) -> tuple[pd.DataFrame, List[Dict]]:
        errors = []
        df_clean = df.copy()
        for field_name, field_info in self.clean_schema.model_fields.items():
            if field_name not in df_clean.columns: continue
            original_series = df_clean[field_name].copy()
            expected_type = str(field_info.annotation)
            
            if 'date' in expected_type: converted_series = pd.to_datetime(original_series, errors='coerce').dt.tz_localize(None)
            elif 'int' in expected_type: converted_series = pd.to_numeric(original_series, errors='coerce').astype('Int64')
            else: converted_series = original_series
            
            error_mask = original_series.notna() & converted_series.isna()
            if error_mask.any():
                error_df = df.loc[error_mask, [self.primary_key, field_name]]
                for _, row in error_df.iterrows():
                    errors.append({"dataset": self.dataset_name, "primary_key": row[self.primary_key], "column": field_name, "original_value": row[field_name], "error": f"Failed to convert to {expected_type}"})
            df_clean[field_name] = converted_series
        return df_clean, errors