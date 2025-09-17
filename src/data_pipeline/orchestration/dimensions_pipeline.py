import logging
from datetime import date, datetime, timezone
from pathlib import Path
from dataclasses import asdict

# --- Import cấu hình ---
from src.data_pipeline.config.pipeline_config import DIMENSIONS_CONFIG, TOKEN_BRANCH_MAP

# --- Import các thành phần Core ---
from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.core.historical_processor import HistoricalProcessor
from src.utils.logger import setup_logging

from src.data_pipeline.pipelines.dimensions.extractor import DimensionExtractor
from src.data_pipeline.pipelines.dimensions.transformer import GenericTransformer
from src.data_pipeline.pipelines.dimensions.processor import GenericDimProcessor

from src.data_pipeline.loaders.staging_loader import StagingLoader, QuarantineLoader
from src.utils.create_historical_schema_from_model import create_historical_schema_from_model

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
# Giảm log nhiễu từ thư viện google
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

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
