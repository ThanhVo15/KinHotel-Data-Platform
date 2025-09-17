from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any
import time

@dataclass
class ProcessingResult:
    name: str
    status: str = "success"
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    input_records: int = 0
    output_records: int = 0
    new_records: int = 0
    updated_records: int = 0
    unchanged_records: int = 0
    output_tables: List[str] = field(default_factory=list)
    error: str | None = None
    duration_seconds: float = 0.0
    data_quality_issues: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return self.status == "success"

class AbstractProcessor(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{self.name}")

    @abstractmethod
    def process(self) -> ProcessingResult:
        pass

    def run(self) -> ProcessingResult:
        self.logger.info(f"🚀 Starting processor: {self.name}...")
        start_time = time.time()
        try:
            result = self.process()
            if result.is_success:
                self.logger.info(f"✅ Processor '{self.name}' completed successfully.")
            else:
                self.logger.error(f"❌ Processor '{self.name}' failed. Reason: {result.error}")
        except Exception as e:
            self.logger.exception(f"💥 Unhandled exception in processor '{self.name}': {e}")
            result = ProcessingResult(name=self.name, status="error", error=str(e))
        
        end_time = time.time()
        result.duration_seconds = end_time - start_time
        self.logger.info(f" Processor '{self.name}' finished in {result.duration_seconds:.2f}s.")
        return result