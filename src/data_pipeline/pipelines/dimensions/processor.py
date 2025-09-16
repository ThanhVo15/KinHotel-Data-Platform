# src/data_pipeline/pipelines/dimensions/processor.py
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Type, Tuple
from pydantic import BaseModel
import json

from ...core.dwh_processor import DWHModelProcessor
from ...core.abstract_processor import ProcessingResult

class GenericDimProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path, dataset_name: str, dwh_table_name: str, dwh_columns: Dict[str, str], clean_schema: Type[BaseModel], primary_key: str):
        super().__init__(
            name=f"GenericDimProcessor-{dataset_name}",
            execution_historical_dir=execution_historical_dir,
            dwh_dir=dwh_dir,
            dataset_name=dataset_name
        )
        self.dataset_name = dataset_name
        self.dwh_table_name = dwh_table_name
        self.dwh_columns = dwh_columns
        self.clean_schema = clean_schema
        self.primary_key = primary_key

    # --- HÀM ĐỂ KIỂM TRA VÀ THU THẬP LỖI ---
    def _enforce_schema_and_collect_errors(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Ép kiểu dữ liệu theo schema và thu thập các dòng bị lỗi.
        """
        errors = []
        df_clean = df.copy()
        
        for field_name, field_info in self.clean_schema.model_fields.items():
            if field_name not in df_clean.columns:
                continue

            original_series = df_clean[field_name].copy()
            expected_type_str = str(field_info.annotation)
            converted_series = None

            # Ép kiểu dữ liệu
            if 'datetime' in expected_type_str or 'date' in expected_type_str:
                converted_series = pd.to_datetime(original_series, errors='coerce').dt.tz_localize(None)
            elif 'int' in expected_type_str:
                converted_series = pd.to_numeric(original_series, errors='coerce').astype('Int64')
            elif 'float' in expected_type_str:
                converted_series = pd.to_numeric(original_series, errors='coerce').astype('float64')
            else: # Mặc định là string/object
                converted_series = original_series
            
            # Tìm lỗi: giá trị gốc không rỗng, nhưng sau khi ép kiểu lại rỗng
            error_mask = original_series.notna() & converted_series.isna()
            if error_mask.any():
                error_df = df.loc[error_mask, [self.primary_key, field_name]]
                self.logger.warning(f"Found {len(error_df)} type conversion errors in column '{field_name}'.")
                for _, row in error_df.iterrows():
                    errors.append({
                        "dataset": self.dataset_name,
                        "primary_key_value": row[self.primary_key],
                        "column": field_name,
                        "original_value": row[field_name],
                        "error_message": f"Failed to convert to {expected_type_str}"
                    })
            
            df_clean[field_name] = converted_series
            
        return df_clean, errors

    def process(self) -> ProcessingResult:
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No current data from historical layer.")

        clean_df, conversion_errors = self._enforce_schema_and_collect_errors(df)
        
        # Đổi tên cột và loại bỏ trùng lặp
        final_df = clean_df.rename(columns=self.dwh_columns)
        dwh_pk = self.dwh_columns.get(self.primary_key, self.primary_key)
        final_df = final_df.drop_duplicates(subset=[dwh_pk]).reset_index(drop=True)
        
        # Lưu kết quả
        output_path = self.dwh_dir / self.dwh_table_name / f"{self.dwh_table_name}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path, index=False)
        
        # Tạo kết quả và đính kèm lỗi Dataaa Quality
        result = ProcessingResult(
            name=self.name,
            input_records=len(df),
            output_records=len(final_df),
            output_tables=[str(output_path)],
            data_quality_issues=conversion_errors
        )
        return result