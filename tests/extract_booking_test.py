import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd

from data_pipeline.extractors.pms.booking import BookingListExtractor
from src.utils.env_utils import get_config
from src.utils.state_manager import load_last_run_timestamp, save_last_run_timestamp

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_extract_booking_with_state_manager():
    """Test trích xuất dữ liệu booking với state management"""
    config = get_config()
    staging_dir = Path(config["paths"]["staging_dir"])
    extractor = BookingListExtractor()
    
    try:
        print("\n===== TEST 1: PARALLEL EXTRACT WITH STATE MANAGER =====")
        
        # Chọn branch để test
        branch_ids = list(extractor.TOKEN_BRANCH_MAP.keys())
        print(f"Testing with branches: {branch_ids}")
        
        # Lưu timestamp hiện tại
        start_time = datetime.now(timezone.utc)
        
        # Tạo danh sách tasks để chạy song song
        tasks = []
        for branch_id in branch_ids:
            # Tạo task không chờ kết quả
            task = extractor.extract_and_save(
                dataset="bookings",
                field="check_in",
                branch_id=branch_id,
                prefer_parquet=True
            )
            tasks.append((branch_id, task))
        
        print(f"Executing {len(tasks)} extraction tasks in parallel...")
        start_exec = time.time()
        
        # Chạy và xử lý kết quả
        for branch_id, task in tasks:
            branch_name = extractor.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
            print(f"\nProcessing branch {branch_id} ({branch_name})...")
            
            # Kiểm tra last_run_timestamp từ state manager
            source_key = "bookings:check_in"
            last_run = load_last_run_timestamp(source_key, branch_id)
            
            if last_run:
                print(f"Found previous run timestamp: {last_run.isoformat()}")
                time_diff = (datetime.now(timezone.utc) - last_run).total_seconds()
                print(f"Time since last run: {time_diff/3600:.2f} hours")
            else:
                print("No previous run found, will extract with default lookback period")
            
            # Chờ task hoàn thành
            try:
                result, staging_path, _ = await task
                
                if result.is_success:
                    print(f"✅ Successfully extracted {result.record_count} records")
                    print(f"   - Saved to: {staging_path}")
                    
                    # Kiểm tra file và dữ liệu
                    if staging_path and os.path.exists(staging_path):
                        try:
                            if staging_path.endswith('.parquet'):
                                df = pd.read_parquet(staging_path)
                            else:
                                df = pd.read_csv(staging_path)
                                
                            print(f"   - File contains {len(df)} records")
                            if 'extracted_at' in df.columns:
                                extracted_times = df['extracted_at'].nunique()
                                print(f"   - Contains {extracted_times} unique extraction timestamps")
                                
                            # Kiểm tra thời gian check_in
                            if 'check_in_datetime' in df.columns:
                                earliest = pd.to_datetime(df['check_in_datetime']).min()
                                latest = pd.to_datetime(df['check_in_datetime']).max()
                                print(f"   - Check-in date range: {earliest} to {latest}")
                        except Exception as e:
                            print(f"   ⚠️ Error reading file: {e}")
                    
                    # Kiểm tra state manager đã cập nhật timestamp mới chưa
                    updated_last_run = load_last_run_timestamp(source_key, branch_id)
                    if updated_last_run and updated_last_run > start_time:
                        print(f"✅ State manager updated with new timestamp: {updated_last_run.isoformat()}")
                    else:
                        print(f"⚠️ State manager may not have updated properly")
                else:
                    print(f"❌ Extraction failed: {result.error}")
            except Exception as e:
                print(f"❌ Exception during extraction: {e}")
        
        elapsed = time.time() - start_exec
        print(f"\nAll extractions completed in {elapsed:.2f} seconds")
        
        print("\n===== TEST 2: CACHED EXTRACTIONS =====")
        print("Running second extract immediately to test caching behavior...")
        
        # Chạy lần thứ 2 để test behavior với những lần chạy gần nhau
        for branch_id in branch_ids[:1]:  # Chỉ test với branch đầu tiên
            branch_name = extractor.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
            print(f"\nSecond extract for branch {branch_id} ({branch_name})...")
            
            # Kiểm tra last_run_timestamp đã được cập nhật
            source_key = "bookings:check_in"
            last_run = load_last_run_timestamp(source_key, branch_id)
            print(f"Current last_run timestamp: {last_run.isoformat() if last_run else 'None'}")
            
            # Extract lần thứ 2
            start_second_run = datetime.now(timezone.utc)
            result, staging_path, _ = await extractor.extract_and_save(
                dataset="bookings",
                field="check_in",
                branch_id=branch_id,
                prefer_parquet=True
            )
            
            if result.is_success:
                print(f"✅ Second extraction completed with {result.record_count} records")
                # Nếu chạy ngay lần 2, có thể sẽ không có bản ghi mới
                if result.record_count == 0:
                    print("   ℹ️ No new records found (expected when running consecutive extractions)")
                
                # Kiểm tra timestamp có được cập nhật không
                updated_last_run = load_last_run_timestamp(source_key, branch_id)
                if updated_last_run and updated_last_run > start_second_run:
                    print(f"✅ State manager updated again: {updated_last_run.isoformat()}")
                else:
                    print(f"ℹ️ State manager kept previous timestamp: {updated_last_run.isoformat() if updated_last_run else 'None'}")
            else:
                print(f"❌ Second extraction failed: {result.error}")
        
        print("\n===== TEST 3: FORCE FULL EXTRACTION =====")
        print("Testing full extraction with explicit date range...")
        
        # Chạy với date range cụ thể thay vì incremental
        for branch_id in branch_ids[:1]:  # Chỉ test với branch đầu tiên
            branch_name = extractor.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
            print(f"\nFull extraction for branch {branch_id} ({branch_name})...")
            
            # Tính toán khoảng thời gian 14 ngày
            now = datetime.now(timezone.utc)
            start_date = now - timedelta(days=14)
            
            print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}")
            
            # Tạo dataset mới để phân biệt với các test trước
            result, staging_path, _ = await extractor.extract_and_save(
                dataset="bookings_full",  # Tên dataset khác để không conflict
                field="check_in_full",    # Field khác để không conflict
                branch_id=branch_id,
                check_in_from=start_date.strftime("%Y-%m-%d %H:%M:%S"),
                check_in_to=now.strftime("%Y-%m-%d %H:%M:%S"),
                prefer_parquet=True
            )
            
            if result.is_success:
                print(f"✅ Full extraction completed with {result.record_count} records")
                print(f"   - Saved to: {staging_path}")
                
                # Kiểm tra file và dữ liệu
                if staging_path and os.path.exists(staging_path):
                    try:
                        if staging_path.endswith('.parquet'):
                            df = pd.read_parquet(staging_path)
                        else:
                            df = pd.read_csv(staging_path)
                            
                        print(f"   - File contains {len(df)} records")
                        
                        # Kiểm tra thời gian check_in nằm trong khoảng yêu cầu
                        if 'check_in_datetime' in df.columns:
                            earliest = pd.to_datetime(df['check_in_datetime']).min()
                            latest = pd.to_datetime(df['check_in_datetime']).max()
                            print(f"   - Check-in date range: {earliest} to {latest}")
                            
                            # Xác nhận dữ liệu nằm trong khoảng đã yêu cầu
                            expected_start = pd.to_datetime(start_date)
                            expected_end = pd.to_datetime(now)
                            
                            if earliest >= expected_start - timedelta(days=1) and latest <= expected_end + timedelta(days=1):
                                print("   ✅ Data is within the requested date range")
                            else:
                                print("   ⚠️ Some data may be outside requested range")
                    except Exception as e:
                        print(f"   ⚠️ Error reading file: {e}")
            else:
                print(f"❌ Full extraction failed: {result.error}")
        
        print("\n===== TEST 4: TEST STATE MANAGER DIRECTLY =====")
        print("Testing the state manager functionality directly...")
        
        test_source = "test_source:check_in"
        test_branch = 999
        test_time = datetime.now(timezone.utc)
        
        # Lưu timestamp mới
        save_last_run_timestamp(test_source, test_branch, test_time)
        print(f"Saved test timestamp: {test_time.isoformat()}")
        
        # Đọc lại để xác nhận
        loaded_time = load_last_run_timestamp(test_source, test_branch)
        
        if loaded_time and abs((loaded_time - test_time).total_seconds()) < 1:
            print("✅ State manager save/load working correctly")
            print(f"   - Saved:  {test_time.isoformat()}")
            print(f"   - Loaded: {loaded_time.isoformat()}")
        else:
            print("❌ State manager issue detected")
            print(f"   - Saved:  {test_time.isoformat()}")
            print(f"   - Loaded: {loaded_time.isoformat() if loaded_time else 'None'}")
        
        print("\n===== TEST 5: CHECK DIRECTORY STRUCTURE =====")
        # Kiểm tra cấu trúc thư mục
        month_str = datetime.now(timezone.utc).strftime("%Y%m")
        expected_structure = staging_dir / "pms" / month_str
        
        if expected_structure.exists():
            print(f"✅ Monthly directory structure exists: {expected_structure}")
            
            # Liệt kê thư mục và file
            all_files = []
            for root, dirs, files in os.walk(expected_structure):
                for file in files:
                    file_path = Path(root) / file
                    rel_path = file_path.relative_to(expected_structure)
                    file_size = os.path.getsize(file_path) / 1024  # KB
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    all_files.append((rel_path, file_size, file_mtime))
            
            # Hiển thị danh sách file, sắp xếp theo thời gian sửa đổi
            all_files.sort(key=lambda x: x[2], reverse=True)
            print(f"Found {len(all_files)} files, showing most recent 5:")
            
            for i, (path, size, mtime) in enumerate(all_files[:5]):
                print(f"   {i+1}. {path} - {size:.1f} KB (modified: {mtime.strftime('%Y-%m-%d %H:%M:%S')})")
        else:
            print(f"❌ Expected directory structure not found: {expected_structure}")
                
    except Exception as e:
        logger.exception(f"Test failed with exception: {e}")
    finally:
        await extractor.close()
        print("\nTest completed - Resources cleaned up")

if __name__ == "__main__":
    print(f"Starting booking extraction test at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    asyncio.run(test_extract_booking_with_state_manager())
    print(f"Test finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")