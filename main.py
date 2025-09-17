import asyncio
import logging
import time
import traceback
import argparse
from datetime import date
from pathlib import Path
from dataclasses import asdict

# --- Import các thành phần Core & Utils ---
from src.utils.logger import setup_logging
from src.utils.env_utils import get_config
from src.data_pipeline.loaders.gdrive_loader import GoogleDriveLoader
from src.data_pipeline.loaders.email_notifier import EmailNotifier

# --- Import "Nhạc trưởng" ---
from src.data_pipeline.orchestration.orchestrator import PipelineOrchestrator

# --- Cấu hình Logging ---
setup_logging('INFO')
logger = logging.getLogger(__name__)
# Giảm log nhiễu từ các thư viện bên ngoài
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


async def main():
    """
    Điểm khởi đầu (Entry Point) của toàn bộ ứng dụng.
    Chịu trách nhiệm phân tích tham số, điều phối và báo cáo.
    """
    parser = argparse.ArgumentParser(description="Chạy các pipeline ETL cho Kin Hotel.")
    parser.add_argument(
        "pipeline", 
        choices=['booking', 'dimensions', 'room_lock', 'odoo', 'all'], 
        help="Tên pipeline cần chạy."
    )
    args = parser.parse_args()

    pipeline_start_time = time.time()
    EXECUTION_DATE = date.today()
    
    report = {
        "execution_date": EXECUTION_DATE.isoformat(), 
        "overall_status": "SUCCESS",
        "pipeline_name": f"'{args.pipeline.upper()}' Pipeline",
        "staging": {}, 
        "historical": {}, 
        "dwh": {}, 
        "gdrive": {}, 
        "error": None,
        "total_time_seconds": 0
    }

    orchestrator = None
    try:
        config = get_config()
        paths = config['paths']
        orchestrator = PipelineOrchestrator(config, report)
        
        # Giao toàn bộ việc chạy pipeline cho "Nhạc trưởng"
        await orchestrator.run(args.pipeline)

        # === GIAI ĐOẠN CUỐI 1: TẢI DỮ LIỆU LÊN GDRIVE ===
        logger.info("\nPHASE FINAL 1: LOADING ALL DATA LAYERS TO GOOGLE DRIVE...")
        gdrive_loader = GoogleDriveLoader(
            sa_path=config['gdrive']['sa_path'], 
            root_folder_id=config['gdrive']['folder_id']
        )
        upload_date_str = EXECUTION_DATE.strftime('%Y-%m-%d')
        layers_to_upload = {
            "staging": paths['staging_dir'], 
            "historical": paths['historical_dir'], 
            "dwh": paths['datawarehouse_dir'],
            "quarantine": paths['quarantine_dir']
        }
        for layer_name, local_path in layers_to_upload.items():
            if Path(local_path).exists():
                gdrive_target_folder = f"data_lake/{layer_name}/{upload_date_str}"
                upload_result = gdrive_loader.load(
                    local_path=str(local_path), 
                    target_subfolder=gdrive_target_folder
                )
                report["gdrive"][layer_name] = asdict(upload_result)
    
    except Exception as e:
        logger.exception(f"💥 PIPELINE FAILED with critical error: {e}")
        report["overall_status"] = "FAILED"
        report["error"] = traceback.format_exc()
    
    finally:
        if orchestrator:
            await orchestrator.close_resources() # Đóng tất cả kết nối (PMS client, etc.)
            
        pipeline_end_time = time.time()
        report["total_time_seconds"] = pipeline_end_time - pipeline_start_time
        
        # === GIAI ĐOẠN CUỐI 2: GỬI EMAIL BÁO CÁO ===
        logger.info("\n--- PHASE FINAL 2: SENDING FINAL REPORT ---")
        email_cfg = config['email']
        email_notifier = EmailNotifier(
            smtp_server="smtp.gmail.com", 
            port=587,
            sender=email_cfg['sender'], 
            password=email_cfg['password'], 
            recipient=email_cfg['recipient']
        )
        email_notifier.load(report_data=report)
        
        final_status_msg = f"🎉 PIPELINE '{args.pipeline.upper()}' FINISHED in {report['total_time_seconds']:.2f}s. Status: {report['overall_status']}"
        logger.info("=" * len(final_status_msg))
        logger.info(final_status_msg)
        logger.info("=" * len(final_status_msg))

if __name__ == "__main__":
    asyncio.run(main())