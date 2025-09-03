# tests/extract_booking_test.py
import os
import sys
import asyncio
import logging
from datetime import datetime
from calendar import monthrange
import pandas as pd

# Đảm bảo PYTHONPATH tới project root
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.data_pipeline.extractors.pms.booking import BookingListExtractor, COLUMNS
from src.utils.date_params import DateWindow, ICT
from src.data_pipeline.extractors.pms.pms_extractor import PMSExtractor  # để dùng TOKEN_BRANCH_MAP

# ---------- CONFIG TEST ----------
YEAR = 2025
MONTH = 8
LIMIT = 100
MAX_CONCURRENT = 5
# Chọn 5 branch (bạn thay list này tuỳ ý)
BRANCH_IDS = [1, 2, 3, 4, 5, 6,7, 9,10]  # ví dụ: 5 branch

# Thư mục output (1 file)
OUTPUT_DIR = os.path.join(ROOT, "data", "staging")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUT_FILE = os.path.join(
    OUTPUT_DIR,
    f"bookings_check_in_{YEAR:04d}-{MONTH:02d}_branch=ALL_extractedAt={datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
)

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tests.extract_booking_test")

def month_range_iCT_strings(year: int, month: int):
    last_day = monthrange(year, month)[1]
    start_str = f"{year:04d}-{month:02d}-01 00:00:00"
    end_str   = f"{year:04d}-{month:02d}-{last_day:02d} 23:59:59"
    return start_str, end_str

async def run_branch(extractor: BookingListExtractor, branch_id: int, dw: DateWindow):
    try:
        res = await extractor.extract_async(
            branch_id=branch_id,
            date_window=dw,
            limit=LIMIT,         # đi thẳng vào params
        )
        if not res or not res.data:
            logger.info(f"[OK(empty)] branch {branch_id} → 0 rows")
            return []
        logger.info(f"[OK] branch {branch_id} → {len(res.data)} rows")
        return res.data
    except Exception as e:
        logger.error(f"[FAIL] branch {branch_id} → {e}")
        return []

async def main():
    logger.info("Hello, Beginning Running extract_booking_test")
    # Chuẩn bị DateWindow cho cả tháng 8 (ICT)
    start_str, end_str = month_range_iCT_strings(YEAR, MONTH)
    # DateWindow nhận datetime; ở đây cho nhanh: dùng tiện ích from_utc với trick:
    # vì API hiểu chuỗi ICT, ta chỉ cần dùng DateWindow để map param đúng, timezone đặt ICT.
    # Tạo datetime từ string để lưu meta cho đẹp:
    start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    end_dt   = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    dw = DateWindow(start=start_dt, end=end_dt, field="check_in", tz=ICT)

    extractor = BookingListExtractor()
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    tasks = []
    async def one(bid: int):
        async with sem:
            return await run_branch(extractor, bid, dw)

    for bid in BRANCH_IDS:
        tasks.append(asyncio.create_task(one(bid)))

    results = await asyncio.gather(*tasks)
    # Gộp và ghi 1 file
    all_rows = []
    for chunk in results:
        if chunk:
            all_rows.extend(chunk)

    if not all_rows:
        logger.warning("No data returned from all selected branches.")
        await extractor.close()
        return

    # Convert → CSV với đúng thứ tự cột (thiếu thì điền None)
    def enforce_columns(row: dict):
        return {col: row.get(col) for col in COLUMNS}

    df = pd.DataFrame([enforce_columns(r) for r in all_rows], columns=COLUMNS)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    logger.info(f"[DONE] ALL → {len(df)} rows → {OUT_FILE}")

    await extractor.close()

if __name__ == "__main__":
    asyncio.run(main())
