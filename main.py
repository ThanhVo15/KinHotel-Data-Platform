# src/data_pipeline/main.py
import asyncio
import logging
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from dataclasses import asdict
import argparse

# --- Import cấu hình ---
from src.data_pipeline.config.pipeline_config import DIMENSIONS_CONFIG, TOKEN_BRANCH_MAP
from src.data_pipeline.schemas.pms_schemas import FactBooking, RoomLock 
from src.data_pipeline.schemas.odoo_schemas import SaleOrderLine

# --- Import các thành phần Core ---
from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.core.clients.odoo_client import OdooClient
from src.data_pipeline.core.historical_processor import HistoricalProcessor
from src.utils.logger import setup_logging
from src.utils.env_utils import get_config

# --- Import các thành phần của Pipeline ---
from src.data_pipeline.pipelines.booking.extractor import BookingExtractor
from src.data_pipeline.pipelines.booking.parser import PMSBookingParser
from src.data_pipeline.pipelines.booking.processors import FactBookingProcessor, DimDateProcessor, DimMarketProcessor

from src.data_pipeline.pipelines.room_lock.extractor import RoomLockExtractor
from src.data_pipeline.pipelines.room_lock.parser import RoomLockParser
from src.data_pipeline.pipelines.room_lock.processors import FactRoomLockProcessor

from src.data_pipeline.pipelines.dimensions.extractor import DimensionExtractor
from src.data_pipeline.pipelines.dimensions.transformer import GenericTransformer
from src.data_pipeline.pipelines.dimensions.processor import GenericDimProcessor

from src.data_pipeline.loaders.staging_loader import StagingLoader, QuarantineLoader
from src.data_pipeline.loaders.gdrive_loader import GoogleDriveLoader
from src.data_pipeline.loaders.email_notifier import EmailNotifier

from src.data_pipeline.pipelines.SaleOrderLine.extractor import OdooSaleOrderExtractor
from src.data_pipeline.pipelines.SaleOrderLine.parser import OdooSaleOrderParser
from src.data_pipeline.pipelines.SaleOrderLine.processors import FactSaleOrderProcessor

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
# Giảm log nhiễu từ thư viện google
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

# --- Hàm Helper ---
def create_historical_schema_from_model(pydantic_model):
    """Tự động tạo dictionary schema cho historical processor từ Pydantic model."""
    schema = {}
    type_mapping = {
        'datetime': 'datetime64[ns, UTC]', 'date': 'datetime64[ns, UTC]',
        'int': 'Int64', 'float': 'float64', 'bool': 'boolean'
    }
    for field_name, field in pydantic_model.model_fields.items():
        type_str = str(field.annotation)
        mapped_type = "object"
        for py_type, pd_type in type_mapping.items():
            if py_type in type_str:
                mapped_type = pd_type
                break
        schema[field_name] = mapped_type
    schema.update({
        "extracted_at": "datetime64[ns, UTC]", "valid_from": "datetime64[ns, UTC]",
        "valid_to": "datetime64[ns, UTC]", "is_current": "boolean", "branch_id": "Int64"
    })
    return schema

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


async def run_dimensions_pipeline(client: PMSClient, config: dict, report: dict):
    """Chạy toàn bộ luồng cho tất cả dimensions trong config."""
    logger.info("="*20 + " BẮT ĐẦU PIPELINE DIMENSIONS " + "="*20)
    EXECUTION_DATE = date.fromisoformat(report["execution_date"])
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR, QUARANTINE_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir'], paths['quarantine_dir']
    
    branch_ids = list(TOKEN_BRANCH_MAP.keys())
    staging_loader = StagingLoader(staging_dir=STAGING_DIR)
    quarantine_loader = QuarantineLoader(quarantine_dir=QUARANTINE_DIR)

    for dim_config in DIMENSIONS_CONFIG:
        dim_name = dim_config['name']
        logger.info(f"--- Bắt đầu xử lý Dimension: {dim_name} ---")

        # 1. EXTRACT
        extractor = DimensionExtractor(dim_config, client)
        multi_branch_results = await extractor.extract_multi_branch(branch_ids=branch_ids)

        successful_branches_for_dim = []
        for branch_id, result in multi_branch_results.items():
            report["staging"][f"{dim_name}-{branch_id}"] = asdict(result)
            if not result.is_success or not result.data:
                continue

            # 2. TRANSFORM & VALIDATE
            transformer = GenericTransformer(dataset_name=dim_name, schema=dim_config['schema'])
            transformed_dfs = transformer.process(
                result.data,
                branch_id=branch_id, 
                extracted_at=datetime.now(timezone.utc)
            )
            
            # 3. LOAD TO STAGING / QUARANTINE
            if not transformed_dfs['clean'].empty:
                staging_loader.load(df=transformed_dfs['clean'], system="pms", dataset=dim_name, field="full", branch_id=branch_id, partition_dt=EXECUTION_DATE)
            if not transformed_dfs['quarantine'].empty:
                quarantine_loader.load(df=transformed_dfs['quarantine'], dataset=dim_name, execution_date=EXECUTION_DATE)

            successful_branches_for_dim.append(branch_id)

        if not successful_branches_for_dim:
            logger.warning(f"Không có dữ liệu thành công cho dimension '{dim_name}'. Bỏ qua các bước sau.")
            continue

        # 4. HISTORICAL PROCESSING
        HISTORICAL_SCHEMA = create_historical_schema_from_model(dim_config['schema'])
        # Chỉ chạy historical cho các branch có dữ liệu mới
        unique_successful_branches = sorted(list(set(successful_branches_for_dim)))
        for branch_id in unique_successful_branches:
            hist_processor = HistoricalProcessor(
                execution_date=EXECUTION_DATE, branch_id=branch_id,
                staging_dir=STAGING_DIR, historical_dir=HISTORICAL_DIR,
                dataset_name=dim_name, primary_key=dim_config['primary_key'],
                columns_to_compare=dim_config['historical_columns'],
                target_schema=HISTORICAL_SCHEMA
            )
            hist_result = hist_processor.run()
            report["historical"][f"{dim_name}-{branch_id}"] = asdict(hist_result)

        # 5. DWH PROCESSING (chỉ chạy 1 lần sau khi tất cả các nhánh đã xử lý historical)
        execution_historical_dir = Path(HISTORICAL_DIR) / EXECUTION_DATE.strftime('%Y/%m/%d')
        dwh_processor = GenericDimProcessor(
            execution_historical_dir=execution_historical_dir, dwh_dir=Path(DWH_DIR),
            dataset_name=dim_name,
            dwh_table_name=dim_config['dwh_table_name'],
            dwh_columns=dim_config['dwh_columns'],
            clean_schema=dim_config['schema'],
            primary_key=dim_config['primary_key']
        )
        dwh_result = dwh_processor.run()
        report["dwh"][dwh_processor.name] = asdict(dwh_result)

        if dwh_result.data_quality_issues:
            if "data_quality_issues" not in report:
                report["data_quality_issues"] = []
            report["data_quality_issues"].extend(dwh_result.data_quality_issues)

    logger.info("✅ PIPELINE DIMENSIONS HOÀN TẤT.")

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

async def run_odoo_pipeline(config: dict, report: dict):
    """Chạy toàn bộ luồng cho pipeline Odoo Sale Order."""
    logger.info("="*20 + " BẮT ĐẦU PIPELINE ODOO SALE ORDER " + "="*20)
    EXECUTION_DATE = date.fromisoformat(report["execution_date"])
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    odoo_cfg = config['odoo']

    client = OdooClient(odoo_cfg['url'], odoo_cfg['email'], odoo_cfg['password'])
    
    try:
        if not await client.login():
            raise Exception("Odoo login failed, aborting pipeline.")

        # === GIAI ĐOẠN 1: EXTRACT ===
        extractor = OdooSaleOrderExtractor(client)
        raw_result = await extractor.extract_async()
        report["staging"]["odoo-sale-order"] = asdict(raw_result)

        if not raw_result.is_success or not raw_result.data:
            logger.warning("Odoo Pipeline: Không có dữ liệu được trích xuất.")
            return

        # === GIAI ĐOẠN 2: PARSE & STAGING ===
        parser = OdooSaleOrderParser()
        parsed_df = parser.parse(raw_result.data)
        
        staging_loader = StagingLoader(staging_dir=STAGING_DIR)
        staging_loader.load(df=parsed_df, system="odoo", dataset="sale_order", field="full", branch_id=0, partition_dt=EXECUTION_DATE)

        # === GIAI ĐOẠN 3: HISTORICAL ===
        HISTORICAL_SCHEMA = create_historical_schema_from_model(SaleOrderLine)
        hist_processor = HistoricalProcessor(
            EXECUTION_DATE, branch_id=0, staging_dir=STAGING_DIR, historical_dir=HISTORICAL_DIR,
            dataset_name="sale_order",
            primary_key="order_line_id",
            columns_to_compare=[k for k in SaleOrderLine.model_fields.keys() if k != 'order_line_id'],
            target_schema=HISTORICAL_SCHEMA
        )
        hist_result = hist_processor.run()
        report["historical"]["odoo-sale-order"] = asdict(hist_result)
        
        # === GIAI ĐOẠN 4: DWH (BƯỚC BỔ SUNG) ===
        logger.info("--- Bắt đầu xử lý Odoo DWH Layer ---")
        execution_historical_dir = Path(HISTORICAL_DIR) / EXECUTION_DATE.strftime('%Y/%m/%d')
        dwh_processor = FactSaleOrderProcessor(execution_historical_dir, Path(DWH_DIR))
        dwh_result = dwh_processor.run()
        report["dwh"][dwh_processor.name] = asdict(dwh_result)
        # ----------------------------------------

    finally:
        await client.close()
    
    logger.info("✅ PIPELINE ODOO SALE ORDER HOÀN TẤT.")

async def main():
    """Hàm điều phối chính: chạy pipeline và gửi báo cáo."""
    parser = argparse.ArgumentParser(description="Chạy các pipeline ETL cho Kin Hotel.")
    parser.add_argument("pipeline", choices=['booking', 'dimensions', 'room_lock', 'SaleOrderLine', 'all'], help="Tên pipeline cần chạy.")
    args = parser.parse_args()

    pipeline_start_time = time.time()
    EXECUTION_DATE = date.today()
    
    report = {
        "execution_date": EXECUTION_DATE.isoformat(), "overall_status": "SUCCESS",
        "pipeline_name": f"'{args.pipeline.upper()}' Pipeline",
        "staging": {}, "historical": {}, "dwh": {}, "gdrive": {}, "error": None,
        "total_time_seconds": 0
    }

    client = None
    try:
        config = get_config()
        paths = config['paths']
        pms_config = config.get('pms', {})
        base_url = (pms_config.get('base_url') or '').rstrip('/') + '/'
        client = PMSClient(base_url)

        if args.pipeline in ['booking', 'all']:
            await run_booking_pipeline(client, config, report)
        
        if args.pipeline in ['dimensions', 'all']:
            await run_dimensions_pipeline(client, config, report)

        if args.pipeline in ['room_lock', 'all']:
            await run_room_lock_pipeline(client, config, report)

        if args.pipeline in ['SaleOrderLine', 'all']:
            await run_odoo_pipeline(config, report)

        # === GIAI ĐOẠN CUỐI: TẢI DỮ LIỆU LÊN GDRIVE ===
        logger.info("\nPHASE FINAL 1: LOADING ALL DATA LAYERS TO GOOGLE DRIVE...")
        gdrive_loader = GoogleDriveLoader(sa_path=config['gdrive']['sa_path'], root_folder_id=config['gdrive']['folder_id'])
        upload_date_str = EXECUTION_DATE.strftime('%Y-%m-%d')
        layers_to_upload = {
            "staging": paths['staging_dir'], 
            "historical": paths['historical_dir'], 
            "dwh": paths['datawarehouse_dir'],
            "quarantine": paths['quarantine_dir']
        }
        for layer_name, local_path in layers_to_upload.items():
            if Path(local_path).exists():
                gdrive_target_folder = f"{layer_name}"
                upload_result = gdrive_loader.load(local_path=str(local_path), target_subfolder=gdrive_target_folder)
                report["gdrive"][layer_name] = asdict(upload_result)
    
    except Exception as e:
        logger.exception(f"💥 PIPELINE FAILED with critical error: {e}")
        report["overall_status"] = "FAILED"
        report["error"] = traceback.format_exc()
    
    finally:
        if client:
            await client.close()
            
        pipeline_end_time = time.time()
        report["total_time_seconds"] = pipeline_end_time - pipeline_start_time
        
        logger.info("\n--- PHASE FINAL 2: SENDING FINAL REPORT ---")
        email_cfg = config['email']
        email_notifier = EmailNotifier(
            smtp_server="smtp.gmail.com", port=587,
            sender=email_cfg['sender'], 
            password=email_cfg['password'], 
            recipient=email_cfg['recipient']
        )
        email_notifier.load(report_data=report)
        
        final_status_msg = f"🎉 PIPELINE '{args.pipeline.upper()}' FINISHED in {report['total_time_seconds']:.2f}s. Status: {report['overall_status']}"
        logger.info("="*len(final_status_msg))
        logger.info(final_status_msg)
        logger.info("="*len(final_status_msg))

if __name__ == "__main__":
    asyncio.run(main())