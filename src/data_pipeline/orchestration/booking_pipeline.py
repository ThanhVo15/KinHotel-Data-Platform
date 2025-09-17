import logging
from datetime import date
from pathlib import Path
from dataclasses import asdict

# --- Import cấu hình ---
from src.data_pipeline.config.pipeline_config import DIMENSIONS_CONFIG, TOKEN_BRANCH_MAP
from src.data_pipeline.schemas.pms_schemas import FactBooking

# --- Import các thành phần Core ---
from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.core.historical_processor import HistoricalProcessor
from src.utils.logger import setup_logging

# --- Import các thành phần của Pipeline ---
from src.data_pipeline.pipelines.booking.extractor import BookingExtractor
from src.data_pipeline.pipelines.booking.parser import PMSBookingParser
from src.data_pipeline.pipelines.booking.processors import FactBookingProcessor, DimDateProcessor, DimMarketProcessor

from src.data_pipeline.loaders.staging_loader import StagingLoader
from src.utils.create_historical_schema_from_model import create_historical_schema_from_model

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
# Giảm log nhiễu từ thư viện google
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

async def run_booking_pipeline(client: PMSClient, config: dict, report: dict):
    """Chạy toàn bộ luồng cho pipeline Booking."""
    logger.info("="*20 + " BẮT ĐẦU PIPELINE BOOKING " + "="*20)
    EXECUTION_DATE = date.fromisoformat(report["execution_date"])
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    
    # === GIAI ĐOẠN 1: EXTRACT -> PARSE -> STAGING ===
    extractor = BookingExtractor(client=client)
    parser = PMSBookingParser()
    staging_loader = StagingLoader(staging_dir=STAGING_DIR)
    
    multi_branch_raw_results = await extractor.extract_multi_branch(
    endpoint=extractor.ENDPOINT,
    field="check_in",
    lookback_days=30
)
    successful_branch_ids = []

    for branch_id, raw_result in multi_branch_raw_results.items():
        report["staging"][f"booking-{branch_id}"] = asdict(raw_result)
        if raw_result.is_success and raw_result.data:
            parsed_df = parser.parse(raw_result.data, branch_id=branch_id)
            staging_loader.load(df=parsed_df, system="pms", dataset="booking", field="check_in", branch_id=branch_id, partition_dt=EXECUTION_DATE)
            successful_branch_ids.append(branch_id)
            
    if not successful_branch_ids:
        logger.warning("Booking Pipeline - Giai đoạn 1: Không có dữ liệu booking mới.")
        return

    # === GIAI ĐOẠN 2: HISTORICAL & DWH ===
    BOOKING_HISTORICAL_SCHEMA = create_historical_schema_from_model(FactBooking)
    for branch_id in successful_branch_ids:
        hist_processor = HistoricalProcessor(
            EXECUTION_DATE, branch_id, STAGING_DIR, HISTORICAL_DIR, "booking", "booking_line_id",
            [k for k in FactBooking.model_fields.keys() if k != 'booking_line_id'], BOOKING_HISTORICAL_SCHEMA
        )
        hist_result = hist_processor.run()
        report["historical"][f"booking-{branch_id}"] = asdict(hist_result)
        if not hist_result.is_success: raise Exception(f"Historical step FAILED for booking branch {branch_id}.")

    execution_historical_dir = Path(HISTORICAL_DIR) / EXECUTION_DATE.strftime('%Y/%m/%d')
    dwh_processors = [
        DimDateProcessor(execution_historical_dir, Path(DWH_DIR)),
        DimMarketProcessor(execution_historical_dir, Path(DWH_DIR)),
        FactBookingProcessor(execution_historical_dir, Path(DWH_DIR)),
    ]
    for processor in dwh_processors:
        dwh_result = processor.run()
        report["dwh"][processor.name] = asdict(dwh_result)
        if not dwh_result.is_success: raise Exception(f"{processor.name} FAILED: {dwh_result.error}")
    
    logger.info("✅ PIPELINE BOOKING HOÀN TẤT.")