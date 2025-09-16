# src/data_pipeline/loaders/staging_loader.py
import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import date, datetime
import logging

from .abstract_loader import AbstractLoader, LoadingResult

class StagingLoader(AbstractLoader):
    """
    Chịu trách nhiệm tải (load) dữ liệu DataFrame vào khu vực Staging.
    Đã được tối ưu để ghi nối tiếp (append) cho file CSV.
    """
    def __init__(self, staging_dir: str):
        super().__init__("StagingLoader")
        self.staging_dir = Path(staging_dir)

    def load(self, *, df: pd.DataFrame, system: str, dataset: str, field: str,
             branch_id: int, partition_dt: Optional[date] = None,
             filename_prefix: Optional[str] = None, prefer_parquet: bool = True) -> LoadingResult:

        result = LoadingResult(name=self.name, target_location=str(self.staging_dir))
        
        if df.empty:
            self.logger.info(f"DataFrame for {dataset}/branch={branch_id} is empty. Skipping load.")
            result.status = "skipped"
            return result

        try:
            system_safe = "".join(ch if ch.isalnum() else "_" for ch in system)
            dataset_safe = "".join(ch if ch.isalnum() else "_" for ch in dataset)
            
            dt = partition_dt or date.today()
            month_str = dt.strftime("%Y%m")
            branch_folder = f"branch={branch_id}"
            out_dir = self.staging_dir / system_safe / month_str / branch_folder / dataset_safe
            out_dir.mkdir(parents=True, exist_ok=True)
            
            prefix = "".join(ch if ch.isalnum() else "_" for ch in filename_prefix) if filename_prefix else dataset_safe
            file_name = f"{prefix}_{field}_branch={branch_id}_monthly"

            # Tối ưu cho CSV
            if not prefer_parquet:
                csv_path = out_dir / f"{file_name}.csv"
                write_header = not csv_path.exists()
                df.to_csv(csv_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                self.logger.info(f"Appended {len(df)} records to {csv_path}")
                result.target_location = str(csv_path)
                result.files_uploaded = 1
                return result

            # Logic cho Parquet
            parquet_path = out_dir / f"{file_name}.parquet"
            if parquet_path.exists():
                existing_df = pd.read_parquet(parquet_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
            else:
                combined_df = df
            
            combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
            self.logger.info(f"Saved {len(df)} new records (total {len(combined_df)}) to {parquet_path}")
            result.target_location = str(parquet_path)
            result.files_uploaded = 1
            return result

        except Exception as e:
            self.logger.exception(f"Failed to load data to staging: {e}")
            result.status = "error"
            result.error = str(e)
            return result

class QuarantineLoader(AbstractLoader):
    """
    Chịu trách nhiệm tải dữ liệu lỗi vào khu vực Kiểm dịch (Quarantine).
    Luôn lưu dưới dạng CSV để dễ dàng đọc và kiểm tra thủ công.
    """
    def __init__(self, quarantine_dir: str = "data/quarantine"):
        super().__init__("QuarantineLoader")
        self.quarantine_dir = Path(quarantine_dir)

    # <<< SỬA TÊN HÀM TỪ `run` THÀNH `load` >>>
    def load(self, *, df: pd.DataFrame, dataset: str, execution_date: date) -> LoadingResult:
        result = LoadingResult(name=self.name, target_location=str(self.quarantine_dir))
        
        if df.empty:
            result.status = "skipped"
            return result

        try:
            date_str = execution_date.strftime("%Y-%m-%d")
            out_dir = self.quarantine_dir / date_str / dataset
            out_dir.mkdir(parents=True, exist_ok=True)
            
            file_name = f"{dataset}_quarantine_{date_str}.csv"
            file_path = out_dir / file_name

            # Ghi đè file mỗi lần chạy vì đây là dữ liệu của ngày hôm đó
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Đã lưu {len(df)} record lỗi vào khu vực kiểm dịch: {file_path}")
            result.files_uploaded = 1
            result.target_location = str(file_path)
        except Exception as e:
            self.logger.exception(f"Thất bại khi lưu dữ liệu kiểm dịch: {e}")
            result.status = "error"
            result.error = str(e)
            
        return result