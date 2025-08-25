# main.py
import asyncio
import logging
from datetime import datetime

# --- CHANGE: Thay đổi đường dẫn này cho đúng với cấu trúc dự án của bạn ---
from data_pipeline.extractors.pms.booking import BookingListExtractor

async def main():
    """
    Hàm chính để chạy pipeline ETL.
    """
    # 1. Thiết lập Logging để theo dõi tiến trình
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger("MAIN_PIPELINE")

    extractor = None
    start_time = datetime.now()
    logger.info("🚀 Starting the main ETL pipeline to extract bookings...")

    try:
        # 2. Khởi tạo Extractor
        # Lớp này giờ đã chứa tất cả logic cần thiết.
        extractor = BookingListExtractor()

        # 3. Chạy quá trình lấy dữ liệu tăng trưởng (Incremental Extraction)
        # BẠN CHỈ CẦN GỌI MỘT HÀM DUY NHẤT NÀY.
        # Nó sẽ tự động:
        #  - Tải timestamp của lần chạy cuối.
        #  - Tính toán khoảng thời gian cần lấy.
        #  - Chuyển đổi múi giờ sang UTC+7 cho API.
        #  - Lấy hết tất cả các trang dữ liệu (pagination).
        #  - Lưu lại timestamp mới sau khi thành công.
        results = await extractor.extract_bookings_incrementally(
            lookback_days=7,  # Chỉ dùng cho lần chạy đầu tiên của mỗi chi nhánh
            limit=100         # Số lượng bản ghi mỗi trang
        )

        # 4. Xử lý và tóm tắt kết quả
        logger.info("--- PIPELINE RESULTS ---")
        total_records = 0
        success_branches = 0
        for branch_id, result in results.items():
            if result.is_success:
                logger.info(f"✅ Branch '{result.branch_name}': SUCCESS - Fetched {result.record_count} records.")
                total_records += result.record_count
                success_branches += 1
                # Tại đây, bạn có thể thêm logic để lưu result.data vào database hoặc file.
                # Ví dụ: await save_to_database(result.data)
            else:
                logger.error(f"❌ Branch '{result.branch_name}': FAILED - Error: {result.error}")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info("--- PIPELINE SUMMARY ---")
        logger.info(f"✅ Successfully extracted from {success_branches}/{len(results)} branches.")
        logger.info(f"📊 Total new records fetched: {total_records}")
        logger.info(f"⏱️ Total pipeline duration: {duration:.2f} seconds.")

    except Exception as e:
        logger.critical(f"💥 A critical error occurred in the pipeline: {e}", exc_info=True)
    finally:
        # 5. LUÔN LUÔN dọn dẹp tài nguyên, dù thành công hay thất bại
        if extractor:
            logger.info("🧹 Cleaning up extractor resources (closing network sessions)...")
            await extractor.close()
            logger.info("✅ Cleanup complete.")

if __name__ == "__main__":
    asyncio.run(main())