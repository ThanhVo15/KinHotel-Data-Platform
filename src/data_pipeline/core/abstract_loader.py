from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass
class LoadingResult:
    """Chuẩn hóa kết quả trả về của mỗi loader."""
    name: str
    status: str = "success"
    loaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    files_uploaded: int = 0
    files_updated: int = 0
    bytes_transferred: int = 0
    target_location: str = ""
    error: str | None = None

    @property
    def is_success(self) -> bool:
        return self.status == "success"

class AbstractLoader(ABC):
    """Lớp cha cho tất cả các bộ tải dữ liệu (sinks)."""
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{self.name}")

    @abstractmethod
    def load(self, *args, **kwargs) -> LoadingResult:
        """
        Phương thức chính để thực thi logic tải dữ liệu.
        Sử dụng *args, **kwargs để linh hoạt cho các loại loader khác nhau.
        """
        pass

    def run(self, *args, **kwargs) -> LoadingResult:
        """Template method để bao bọc việc thực thi."""
        self.logger.info(f"🚀 Starting loader: {self.name}...")
        try:
            result = self.load(*args, **kwargs)
            if result.is_success:
                self.logger.info(f"✅ Loader '{self.name}' completed successfully.")
            else:
                self.logger.error(f"❌ Loader '{self.name}' failed. Reason: {result.error}")
            return result
        except Exception as e:
            self.logger.exception(f"💥 Unhandled exception in loader '{self.name}': {e}")
            return LoadingResult(name=self.name, status="error", error=str(e))