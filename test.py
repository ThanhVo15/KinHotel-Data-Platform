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

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

# <<< KHÔI PHỤC LẠI LOGIC TỰ ĐỘNG ĐIỀN CONFIG >>>
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

async def run_dimensions_pipeline(config: Dict[str, Any], execution_date: date, pipeline_report: Dict[str, Any], dimensions_to_test: List[str]):
    logger.info("="*20 + " BẮT ĐẦU PIPELINE DIMENSIONS (CHẾ ĐỘ TEST) " + "="*19)
    paths = config['paths']
    STAGING_DIR, HISTORICAL_DIR, DWH_DIR = paths['staging_dir'], paths['historical_dir'], paths['datawarehouse_dir']
    execution_dt_utc = datetime.now(timezone.utc)
    quarantine_loader = QuarantineLoader(quarantine_dir="data/quarantine")

    for dim_config in DIMENSIONS_CONFIG:
        dataset_name = dim_config['name']
        if dataset_name not in dimensions_to_test:
            continue

        logger.info(f"--- Bắt đầu xử lý Dimension: {dataset_name.upper()} ---")
        ExtractorClass = dim_config['load_strategy'] # Chú ý: Key này có thể là 'extractor_class' tùy vào file config của bạn
        extractor = DimensionExtractor(endpoint=dim_config['endpoint'], strategy=ExtractorClass)
        
        try:
            results = await extractor.extract_multi_branch()
            raw_data = next(iter(results.values())).data if results else []
            if not raw_data:
                logger.warning(f"Không có dữ liệu cho {dataset_name}, bỏ qua.")
                continue

            # Khởi tạo Transformer
            transformer = GenericTransformer(
                dataset_name=dataset_name, 
                schema=dim_config['schema']
            )

            # <<< SỬA LỖI: Gọi thẳng vào process() thay vì run() >>>
            processed_dfs = transformer.process(
                raw_data=raw_data,
                extracted_at=execution_dt_utc
            )

            clean_df, quarantine_df = processed_dfs["clean"], processed_dfs["quarantine"]

            if not quarantine_df.empty:
                quarantine_loader.run(df=quarantine_df, dataset=dataset_name, execution_date=execution_date)
            if clean_df.empty:
                logger.warning(f"Không có dữ liệu sạch cho {dataset_name}, bỏ qua.")
                continue

            staging_loader = StagingLoader(staging_dir=STAGING_DIR)
            for branch_id in extractor.TOKEN_BRANCH_MAP.keys():
                df_for_branch = clean_df.copy()
                df_for_branch['branch_id'] = branch_id
                staging_loader.run(df=df_for_branch, system="pms", dataset=dataset_name, field="full", branch_id=branch_id, partition_dt=execution_date)
            
            hist_schema = create_historical_schema_from_model(dim_config['schema'])
            hist_processor = HistoricalProcessor(
                execution_date, branch_id, STAGING_DIR, HISTORICAL_DIR,
                dataset_name, dim_config['primary_key'],
                dim_config['historical_columns'], hist_schema
            )
            hist_result = hist_processor.run()
            pipeline_report["historical"][dataset_name] = asdict(hist_result)
            
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
    logger.info("="*20 + " KẾT THÚC PIPELINE DIMENSIONS (CHẾ ĐỘ TEST) " + "="*18)

async def main():
    pipeline_start_time = time.time()
    EXECUTION_DATE = date.today()
    
    DIMENSIONS_TO_TEST = ["customers", "branches", "travel_agencies"]
    logger.info(f"CHẠY PIPELINE Ở CHẾ ĐỘ TEST CHO CÁC DIMENSIONS: {DIMENSIONS_TO_TEST}")
    
    pipeline_report = {
        "execution_date": EXECUTION_DATE.isoformat(), "overall_status": "SUCCESS", 
        "data_quality_issues": [], "staging": {}, "historical": {}, "dwh": {}, 
        "gdrive": {}, "error": None, "total_time_seconds": 0
    }
    try:
        config = get_config()
        
        # logger.info("="*20 + " BỎ QUA PIPELINE BOOKING (CHẾ ĐỘ TEST) " + "="*20)
        # await run_booking_pipeline(config, EXECUTION_DATE, pipeline_report)

        await run_dimensions_pipeline(config, EXECUTION_DATE, pipeline_report, DIMENSIONS_TO_TEST)

        # logger.info("\nPHASE FINAL 1: BỎ QUA TẢI LÊN GOOGLE DRIVE (CHẾ ĐỘ TEST)...")
    except Exception as e:
        logger.exception(f"💥 PIPELINE TEST FAILED with critical error: {e}")
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
        email_notifier.run(report_data=pipeline_report)
        logger.info("="*60)
        logger.info(f"🎉 PIPELINE TEST FINISHED in {pipeline_report['total_time_seconds']:.2f}s. Status: {pipeline_report['overall_status']}")
        logger.info("="*60)

if __name__ == "__main__":
    asyncio.run(main())