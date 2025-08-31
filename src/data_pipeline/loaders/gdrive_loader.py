# src/loaders/gdrive_loader.py
from __future__ import annotations
import io
import math
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from src.data_pipeline.loaders.abstract_loader import AbstractLoader, LoadArtifact, LoadSummary
from src.utils.gdrive_client import GoogleDriveClient

logger = logging.getLogger(__name__)

def _iter_batches(records: Iterable[Dict[str, Any]], batch_size: int):
    """Yield lists of up to batch_size items."""
    batch: List[Dict[str, Any]] = []
    for rec in records:
        batch.append(rec)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")

class GDriveSilverLoader(AbstractLoader):
    """
    Loader Silver:
      - Lưu PARQUET & SHEET lên Google Drive (trong: silver/pms/{parquet|sheet}/bookings/branch=.../dt=YYYYMMDD/)
      - Upload theo batch (parquet_batch_size, sheet_batch_size)
      - Upload song song các batch (max_workers)
    """

    def __init__(self, drive: GoogleDriveClient, *, product_root=("silver", "pms")) -> None:
        self.drive = drive
        self.product_root = list(product_root)

    def load(
        self,
        *,
        dataset: str,                   # "bookings"
        branch_id: Optional[int],
        records: Iterable[Dict[str, Any]],
        parquet_batch_size: int = 100_000,
        sheet_batch_size: int = 50_000,
        max_workers: int = 4,
    ) -> LoadSummary:
        records = list(records) 
        # Chuẩn bị folder path
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        # Parquet path
        parquet_path = self.product_root + ["parquet", dataset, f"branch={branch_id}", f"dt={today}"]
        sheet_path   = self.product_root + ["sheet",   dataset, f"branch={branch_id}", f"dt={today}"]

        parquet_folder_id = self.drive.ensure_folder_path(parquet_path)
        sheet_folder_id   = self.drive.ensure_folder_path(sheet_path)

        # Chia records cho 2 luồng batch khác nhau (tránh giữ tất cả trong bộ nhớ)
        # => Ta sẽ duyệt records 2 lần. Nếu bạn muốn chỉ duyệt 1 lần, có thể cache ra file tạm,
        #    nhưng phương án này đơn giản và đủ nhanh trong đa số ETL.
        artifacts: List[LoadArtifact] = []

        # -------- PARQUET UPLOAD --------
        logger.info("Start uploading PARQUET batches...")
        parquet_jobs = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for i, batch in enumerate(_iter_batches(records, parquet_batch_size), start=1):
                # Convert batch -> Parquet bytes
                df = pd.DataFrame.from_records(batch)
                bio = io.BytesIO()
                df.to_parquet(bio, index=False)  # yêu cầu pyarrow
                data = bio.getvalue()

                fname = f"{dataset}_{_utc_stamp()}_part{i}.parquet"
                fut = pool.submit(self.drive.upload_parquet_bytes, data, parquet_folder_id, fname)
                parquet_jobs.append((fut, len(df), {"batch": i}))

            for fut, rows, meta in as_completed([j[0] for j in parquet_jobs], timeout=None):
                pass  # join all futures first to keep order independent

            # Re-collect results with order
            for fut, rows, meta in parquet_jobs:
                res = fut.result()
                artifacts.append(LoadArtifact(
                    kind="parquet",
                    id=res["id"],
                    name=res["name"],
                    link=res["webViewLink"],
                    rows=rows,
                    meta=meta
                ))

        # -------- SHEET UPLOAD --------
        logger.info("Start uploading SHEET batches...")
        sheet_jobs = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            part_index = 0
            # Lưu ý: records đã được iterator tiêu thụ ở vòng parquet. Cần records là list hoặc re-iterable.
            # => BẮT BUỘC: truyền vào đây là list (ví dụ tres.data). Nếu là generator, hãy list() trước khi load.
            # Để an toàn: ép cast về list nếu chưa phải list.
            if not isinstance(records, list):
                records = list(records)

            for i, batch in enumerate(_iter_batches(records, sheet_batch_size), start=1):
                df = pd.DataFrame.from_records(batch)

                # Google Drive convert CSV -> Sheet
                csv_bio = io.BytesIO()
                df.to_csv(csv_bio, index=False, encoding="utf-8")
                csv_bytes = csv_bio.getvalue()

                fname = f"{dataset}_branch{branch_id}_{_utc_stamp()}_part{i}"
                fut = pool.submit(self.drive.upload_csv_as_sheet, csv_bytes, sheet_folder_id, fname)
                sheet_jobs.append((fut, len(df), {"batch": i}))

            for fut, rows, meta in as_completed([j[0] for j in sheet_jobs], timeout=None):
                pass

            for fut, rows, meta in sheet_jobs:
                res = fut.result()
                artifacts.append(LoadArtifact(
                    kind="sheet",
                    id=res["id"],
                    name=res["name"],
                    link=res["webViewLink"],
                    rows=rows,
                    meta=meta
                ))

        return LoadSummary(dataset=dataset, branch_id=branch_id, artifacts=artifacts)
