import logging
from datetime import date
from pathlib import Path
from dataclasses import asdict

# --- Import cấu hình ---
from src.data_pipeline.config.pipeline_config import DIMENSIONS_CONFIG, TOKEN_BRANCH_MAP
from src.data_pipeline.schemas.pms_schemas import RoomLock 

# --- Import các thành phần Core ---
from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.core.historical_processor import HistoricalProcessor
from src.utils.logger import setup_logging

from src.data_pipeline.pipelines.room_lock.extractor import RoomLockExtractor
from src.data_pipeline.pipelines.room_lock.parser import RoomLockParser
from src.data_pipeline.pipelines.room_lock.processors import FactRoomLockProcessor

from src.data_pipeline.loaders.staging_loader import StagingLoader
from src.utils.create_historical_schema_from_model import create_historical_schema_from_model

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
# Giảm log nhiễu từ thư viện google
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

async def run_room_lock_pipeline(client: PMSClient, config: dict, report: dict):
    """Chạy toàn bộ luồng cho pipeline Room Lock."""
    logger.info("="*20 + " BẮT ĐẦU PIPELINE ROOM LOCK " + "="*20)
    EXECUTION_DATE = date.fromisoformat(report["execution_date"])
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    
    # === GIAI ĐOẠN 1: EXTRACT -> PARSE -> STAGING ===
    extractor = RoomLockExtractor(client=client)
    parser = RoomLockParser()
    staging_loader = StagingLoader(staging_dir=STAGING_DIR)
    
    # Tận dụng logic incremental load từ lớp cha, dùng trường 'start_date'
    multi_branch_raw_results = await extractor.extract_multi_branch(
        endpoint=extractor.ENDPOINT,
        field="start_date", # Lấy tăng trưởng theo start_date
        lookback_days=90 # Nhìn lại 90 ngày và cả tương lai để bắt các thay đổi
    )
    successful_branch_ids = []

    for branch_id, raw_result in multi_branch_raw_results.items():
        report["staging"][f"room_lock-{branch_id}"] = asdict(raw_result)
        if raw_result.is_success and raw_result.data:
            parsed_df = parser.parse(raw_result.data, branch_id=branch_id)
            staging_loader.load(df=parsed_df, system="pms", dataset="room_lock", field="start_date", branch_id=branch_id, partition_dt=EXECUTION_DATE)
            successful_branch_ids.append(branch_id)
            
    if not successful_branch_ids:
        logger.warning("Room Lock Pipeline - Giai đoạn 1: Không có dữ liệu mới.")
        return

    # === GIAI ĐOẠN 2: HISTORICAL ===
    HISTORICAL_SCHEMA = create_historical_schema_from_model(RoomLock)
    for branch_id in successful_branch_ids:
        hist_processor = HistoricalProcessor(
            EXECUTION_DATE, branch_id, STAGING_DIR, HISTORICAL_DIR,
            dataset_name="room_lock",
            primary_key="id",
            columns_to_compare=[k for k in RoomLock.model_fields.keys() if k != 'id'],
            target_schema=HISTORICAL_SCHEMA
        )
        hist_result = hist_processor.run()
        report["historical"][f"room_lock-{branch_id}"] = asdict(hist_result)
    
    # === GIAI ĐOẠN 3: DWH ===
    logger.info("--- Bắt đầu xử lý Room Lock DWH Layer ---")
    execution_historical_dir = Path(HISTORICAL_DIR) / EXECUTION_DATE.strftime('%Y/%m/%d')
    dwh_processor = FactRoomLockProcessor(execution_historical_dir, Path(DWH_DIR))
    dwh_result = dwh_processor.run()
    report["dwh"][dwh_processor.name] = asdict(dwh_result)
    logger.info("✅ PIPELINE ROOM LOCK HOÀN TẤT.")