# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\utils\logger.py
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# ANSI colors cho log
COLORS = {
    'DEBUG': '\033[90m',    # Xám
    'INFO': '\033[32m',     # Xanh lá
    'WARNING': '\033[33m',  # Vàng
    'ERROR': '\033[31m',    # Đỏ
    'CRITICAL': '\033[41m', # Nền đỏ
    'RESET': '\033[0m'      # Reset màu
}

class ColoredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_file_name = os.path.basename(record.pathname)
        level_color = COLORS.get(record.levelname, COLORS['RESET'])
        message = super().format(record)

        return  f"[{timestamp}] [{current_file_name}] {level_color}[{record.levelname}] {message}{COLORS['RESET']}"

def setup_logging(level: str ='INFO') -> None:
    logger = logging.getLogger()
    logger.setLevel(level)

    logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    formatter = ColoredFormatter('%(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

if __name__ == "__main__":
    print("🚀 Bắt đầu test logging...")
    
    setup_logging('DEBUG')
    
    logger = logging.getLogger()
    logger.debug("Test Debug: Xám")
    logger.info("Test INFO: Xanh")
    logger.warning("Test WARNING: Vàng")
    logger.error("Test ERROR: Đỏ")
    logger.critical("Test CRITICAL: Nền đỏ")
    
    print("✅ Test hoàn thành!")