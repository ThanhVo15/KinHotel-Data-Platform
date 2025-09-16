import asyncio
import logging
import time
import traceback
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import asdict

# --- Import các thành phần của pipeline ---
from src.utils.logger import setup_logging
from src.utils.env_utils import get_config
from src.data_pipeline.config.pms_dimensions_config import DIMENSIONS_CONFIG
from src.data_pipeline.schemas.pms_schemas import FactBooking
from src.data_pipeline.extractors.pms.booking import BookingListExtractor
from src.data_pipeline.extractors.pms.generic_dimension_extractors import DimensionExtractor
from src.data_pipeline.parsers.pms_booking_parser import PMSBookingParser
from src.data_pipeline.processors.generic_processors import GenericTransformer, GenericDimProcessor
from src.data_pipeline.loaders.staging_loader import StagingLoader, QuarantineLoader
from src.data_pipeline.loaders.gdrive_loader import GoogleDriveLoader
from src.data_pipeline.loaders.email_notifier import EmailNotifier
from src.data_pipeline.processors.historical_processor import HistoricalProcessor
from src.data_pipeline.processors.dwh.dim_date_processor import DimDateProcessor
from src.data_pipeline.processors.dwh.dim_market_processor import DimMarketProcessor
from src.data_pipeline.processors.dwh.fact_booking_processor import FactBookingProcessor

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

# --- Tự động điền thông tin config ---
for dim in DIMENSIONS_CONFIG:
    dim["historical_columns"] = list(dim["schema"].model_fields.keys())
    dim["dwh_table_name"] = f"dim_{dim['name']}"
    dim["dwh_columns"] = {col: col for col in dim["schema"].model_fields.keys()}

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

async def run_booking_pipeline(config: Dict[str, Any], execution_date: date, pipeline_report: Dict[str, Any]):
    """Chạy pipeline ETL chuyên biệt cho dữ liệu Booking (Fact Table)."""
    logger.info("="*20 + " BẮT ĐẦU PIPELINE BOOKING " + "="*20)
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    successful_branch_ids = []
    extractor = BookingListExtractor()
    try:
        parser = PMSBookingParser()
        staging_loader = StagingLoader(staging_dir=STAGING_DIR)
        multi_branch_raw_results = await extractor.extract_multi_branch(field="check_in", lookback_days=30)
        
        for branch_id, raw_result in multi_branch_raw_results.items():
            pipeline_report["staging"][f"booking-{branch_id}"] = asdict(raw_result)
            if raw_result.is_success and raw_result.data:
                parsed_df = parser.parse(raw_result.data, branch_id=branch_id)
                staging_loader.run(df=parsed_df, system="pms", dataset="booking", field="check_in", branch_id=branch_id)
                successful_branch_ids.append(branch_id)
        
        if not successful_branch_ids:
            logger.warning("Booking Pipeline - Giai đoạn 1: Không có dữ liệu booking mới.")
            return

        BOOKING_HISTORICAL_SCHEMA = create_historical_schema_from_model(FactBooking)
        for branch_id in successful_branch_ids:
            hist_processor = HistoricalProcessor(
                execution_date, branch_id, STAGING_DIR, HISTORICAL_DIR, "booking", "booking_line_id",
                [k for k in FactBooking.model_fields.keys() if k != 'booking_line_id'], BOOKING_HISTORICAL_SCHEMA
            )
            hist_result = hist_processor.run()
            pipeline_report["historical"][f"booking-{branch_id}"] = asdict(hist_result)
            if not hist_result.is_success: raise Exception(f"Historical step FAILED for booking branch {branch_id}.")
        
        execution_historical_dir = Path(HISTORICAL_DIR) / execution_date.strftime('%Y/%m/%d')
        dwh_processors = [
            DimDateProcessor(execution_historical_dir, Path(DWH_DIR)),
            DimMarketProcessor(execution_historical_dir, Path(DWH_DIR)),
            FactBookingProcessor(execution_historical_dir, Path(DWH_DIR)),
        ]
        for processor in dwh_processors:
            dwh_result = processor.run()
            pipeline_report["dwh"][processor.name] = asdict(dwh_result)
            if not dwh_result.is_success: raise Exception(f"{processor.name} FAILED: {dwh_result.error}")
    finally:
        await extractor.close()
        logger.info("="*20 + " KẾT THÚC PIPELINE BOOKING " + "="*21)

async def run_dimensions_pipeline(config: Dict[str, Any], execution_date: date, pipeline_report: Dict[str, Any]):
    logger.info("="*20 + " BẮT ĐẦU PIPELINE DIMENSIONS " + "="*19)
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    execution_dt_utc = datetime.now(timezone.utc)
    quarantine_loader = QuarantineLoader(quarantine_dir="data/quarantine")

    for dim_config in DIMENSIONS_CONFIG:
        dataset_name = dim_config['name']
        logger.info(f"--- Bắt đầu xử lý Dimension: {dataset_name.upper()} ---")
        
        extractor = DimensionExtractor(endpoint=dim_config['endpoint'], strategy=dim_config['load_strategy'])
        try:
            results = await extractor.extract_multi_branch()
            raw_data = next(iter(results.values())).data if results else []
            if not raw_data:
                logger.warning(f"Không có dữ liệu cho {dataset_name}, bỏ qua.")
                continue

            transformer = GenericTransformer(dataset_name=dataset_name, schema=dim_config['schema'])
            processed_dfs = transformer.process(raw_data=raw_data, extracted_at=execution_dt_utc)
            clean_df, quarantine_df = processed_dfs["clean"], processed_dfs["quarantine"]

            if not quarantine_df.empty:
                quarantine_loader.load(df=quarantine_df, dataset=dataset_name, execution_date=execution_date)
            
            if clean_df.empty:
                logger.warning(f"Không có dữ liệu sạch cho {dataset_name} sau khi transform, bỏ qua.")
                continue

            staging_loader = StagingLoader(staging_dir=STAGING_DIR)
            for branch_id in extractor.TOKEN_BRANCH_MAP.keys():
                df_for_branch = clean_df.copy()
                df_for_branch['branch_id'] = branch_id
                staging_loader.load(df=df_for_branch, system="pms", dataset=dataset_name, field="full", branch_id=branch_id, partition_dt=execution_date)

                hist_schema = create_historical_schema_from_model(dim_config['schema'])
                hist_processor = HistoricalProcessor(
                    execution_date, branch_id, STAGING_DIR, HISTORICAL_DIR,
                    dataset_name, dim_config['primary_key'],
                    dim_config['historical_columns'], hist_schema
                )
                hist_result = hist_processor.run()
                pipeline_report["historical"][f"{dataset_name}-{branch_id}"] = asdict(hist_result)
            
            dwh_proc = GenericDimProcessor(
                Path(HISTORICAL_DIR) / execution_date.strftime('%Y/%m/%d'), Path(DWH_DIR),
                dataset_name, dim_config['dwh_table_name'],
                dim_config['dwh_columns'], dim_config['schema'], dim_config['primary_key']
            )
            dwh_result = dwh_proc.run()
            pipeline_report["dwh"][dwh_proc.name] = asdict(dwh_result)

            if dwh_result.error and dwh_result.status == "success":
                try:
                    errors = json.loads(dwh_result.error)
                    pipeline_report["data_quality_issues"].extend(errors)
                    dwh_result.error = None
                except (json.JSONDecodeError, TypeError): pass
            if not dwh_result.is_success:
                raise Exception(f"{dwh_proc.name} FAILED: {dwh_result.error}")
        finally:
            await extractor.close()
    logger.info("="*20 + " KẾT THÚC PIPELINE DIMENSIONS " + "="*18)

async def main():
    pipeline_start_time = time.time()
    EXECUTION_DATE = date.today()
    pipeline_report = {
        "execution_date": EXECUTION_DATE.isoformat(), "overall_status": "SUCCESS", 
        "data_quality_issues": [], "staging": {}, "historical": {}, "dwh": {}, 
        "gdrive": {}, "error": None, "total_time_seconds": 0
    }
    try:
        config = get_config()
        paths = config['paths']
        
        await run_booking_pipeline(config, EXECUTION_DATE, pipeline_report)
        await run_dimensions_pipeline(config, EXECUTION_DATE, pipeline_report)

        logger.info("\nPHASE FINAL 1: LOADING ALL DATA LAYERS TO GOOGLE DRIVE...")
        gdrive_loader = GoogleDriveLoader(sa_path=config['gdrive']['sa_path'], root_folder_id=config['gdrive']['folder_id'])
        upload_date_str = EXECUTION_DATE.strftime('%Y-%m-%d')
        layers_to_upload = {
            "staging": paths['staging_dir'], 
            "historical": paths['historical_dir'], 
            "dwh": paths['datawarehouse_dir'], 
            "quarantine": "data/quarantine"
        }
        for layer_name, local_path in layers_to_upload.items():
            if Path(local_path).exists():
                gdrive_target_folder = f"{layer_name}"
                upload_result = gdrive_loader.load(local_path=str(local_path), target_subfolder=gdrive_target_folder)
                pipeline_report["gdrive"][layer_name] = asdict(upload_result)
    except Exception as e:
        logger.exception(f"💥 PIPELINE FAILED with critical error: {e}")
        pipeline_report["overall_status"] = "FAILED"
        pipeline_report["error"] = traceback.format_exc()
    finally:
        pipeline_end_time = time.time()
        pipeline_report["total_time_seconds"] = pipeline_end_time - pipeline_start_time
        logger.info("\n--- PHASE FINAL 2: SENDING FINAL REPORT ---")
        email_notifier = EmailNotifier(
            smtp_server="smtp.gmail.com", port=587,
            sender=config['email']['sender'], password=config['email']['password'], recipient=config['email']['recipient']
        )
        email_notifier.load(report_data=pipeline_report)
        logger.info("="*60)
        logger.info(f"🎉 PIPELINE FINISHED in {pipeline_report['total_time_seconds']:.2f}s. Status: {pipeline_report['overall_status']}")
        logger.info("="*60)

if __name__ == "__main__":
    asyncio.run(main())