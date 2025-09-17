import logging
from datetime import date
from pathlib import Path
from dataclasses import asdict

# --- Import cấu hình ---
from src.data_pipeline.config.pipeline_config import DIMENSIONS_CONFIG, TOKEN_BRANCH_MAP

from src.data_pipeline.core.clients.odoo_client import OdooClient
from src.utils.logger import setup_logging

from src.data_pipeline.loaders.staging_loader import StagingLoader

from src.data_pipeline.pipelines.SaleOrderLine.extractor import OdooSaleOrderExtractor
from src.data_pipeline.pipelines.SaleOrderLine.parser import OdooSaleOrderParser
from src.data_pipeline.pipelines.SaleOrderLine.processors import FactSaleOrderProcessor
from ..schemas.odoo_schemas import SaleOrderLine
from src.utils.create_historical_schema_from_model import create_historical_schema_from_model

logger = logging.getLogger(__name__)

async def run_odoo_pipeline(config: dict, report: dict):
    """
    Chạy toàn bộ luồng cho pipeline Odoo Sale Order (Chiến lược Ghi đè).
    """
    logger.info("="*20 + " BẮT ĐẦU PIPELINE ODOO SALE ORDER " + "="*20)
    EXECUTION_DATE = date.fromisoformat(report["execution_date"])
    paths = config['paths']
    STAGING_DIR, DWH_DIR = paths['staging_dir'], paths['datawarehouse_dir']
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

        # === BỎ QUA GIAI ĐOẠN HISTORICAL ===
        
        # === GIAI ĐOẠN 3: DWH (GHI ĐÈ) ===
        logger.info("--- Bắt đầu xử lý Odoo DWH Layer (Overwrite) ---")
        dwh_processor = FactSaleOrderProcessor(dwh_dir=Path(DWH_DIR))
        dwh_result = dwh_processor.process(staging_df=parsed_df) 
        report["dwh"][dwh_processor.name] = asdict(dwh_result)

    finally:
        await client.close()
    
    logger.info("✅ PIPELINE ODOO SALE ORDER HOÀN TẤT.")