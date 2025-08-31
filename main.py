from src.data_pipeline.extractors.pms.booking import BookingListExtractor
from src.utils.date_params import DateWindow
from src.data_pipeline.transformers.pms.fact_booking import FactBookingTransformer
from src.utils.gdrive_client import GoogleDriveClient
from src.data_pipeline.loaders.gdrive_loader import GDriveSilverLoader
from src.utils.emailer import send_etl_summary_email, render_summary_html
from src.utils.env_utils import get_config

import asyncio

# main.py (đoạn bạn đưa – cập nhật nhẹ)

async def run_once(branch_ids):
    cfg = get_config()
    root_id = cfg["gdrive"]["folder_id"]
    drive = GoogleDriveClient(root_folder_id=root_id)
    loader = GDriveSilverLoader(drive)

    extractor = BookingListExtractor()
    transformer = FactBookingTransformer()  # hoặc BookingTransformer nếu bạn dùng tên cũ

    dw = DateWindow.lookback_days(1, field="check_in")

    run_summaries = []

    try:
        for b in branch_ids:
            try:
                # 1) Extract
                ext = await extractor.extract_async(branch_id=b, date_window=dw, limit=15)
                if not ext.is_success or not ext.data:
                    # Ghi nhận branch lỗi nhưng không dừng toàn job
                    run_summaries.append({
                        "dataset": "bookings",
                        "branch_id": b,
                        "records_out": 0,
                        "status": ext.status,
                        "artifacts": [],
                    })
                    continue

                # 2) Transform
                tres = transformer.transform(
                    ext.data,
                    branch_id=ext.branch_id,
                    window_start=ext.created_date_from,
                    window_end=ext.created_date_to
                )

                # 3) Load
                if tres.data:
                    summary = loader.load(
                        dataset="bookings",
                        branch_id=ext.branch_id,
                        records=tres.data,          # list
                        parquet_batch_size=100_000,
                        sheet_batch_size=50_000,
                        max_workers=4
                    )
                    artifacts = [
                        {"kind": a.kind, "name": a.name, "link": a.link, "rows": a.rows}
                        for a in summary.artifacts
                    ]
                else:
                    artifacts = []

                run_summaries.append({
                    "dataset": "bookings",
                    "branch_id": ext.branch_id,
                    "records_out": tres.records_out,
                    "status": tres.status,
                    "artifacts": artifacts
                })

            except PermissionError as pe:
                # 401/403 của PMS
                run_summaries.append({
                    "dataset": "bookings",
                    "branch_id": b,
                    "records_out": 0,
                    "status": f"error:{type(pe).__name__}",
                    "artifacts": [],
                })
            except Exception as e:
                run_summaries.append({
                    "dataset": "bookings",
                    "branch_id": b,
                    "records_out": 0,
                    "status": f"error:{type(e).__name__}",
                    "artifacts": [],
                })
    finally:
        # Đảm bảo đóng session aiohttp
        await extractor.close()

    # 5) Email tổng kết
    html = render_summary_html("KIN PMS ETL - Daily Load (Silver)", run_summaries)
    send_etl_summary_email(
        subject="KIN PMS ETL - Silver Load",
        html_body=html,
        sender=cfg["email"]["sender"],
        password=cfg["email"]["password"],
        recipient=cfg["email"]["recipient"],
    )


if __name__ == "__main__":
    asyncio.run(run_once([1,2,3,4,5,6,7,9,10]))


