# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\data_pipeline\extractors\abstract_extractor.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, List
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, date
from pathlib import Path
import json

import pandas as pd
from ...utils.env_utils import get_config


@dataclass
class ExtractionResult:
    """Template for extraction results. Holds data and metadata."""
    data: Any
    source: str
    branch_id: Optional[int] = None
    branch_name: Optional[str] = None
    extracted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    record_count: int = 0
    status: str = "success"
    error: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    update_from: Optional[datetime] = None
    update_to: Optional[datetime] = None

    @property
    def is_success(self) -> bool:
        return self.status == "success" and self.error is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "branch_id": self.branch_id,
            "branch_name": self.branch_name,
            "extracted_at": self.extracted_at.isoformat(),
            "record_count": self.record_count,
            "status": self.status,
            "error": self.error,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "update_from": self.update_from.isoformat() if self.update_from else None,
            "update_to": self.update_to.isoformat() if self.update_to else None,
        }


class AbstractExtractor(ABC):
    """
    Base class cho mọi extractor.
    Vòng đời chuẩn (Phase 2):
      Extract (async) → (Flatten ở lớp con) → Save to Staging (template method)
    """

    def __init__(self, name: str):
        self.name = name   # ví dụ "PMS", "ODOO"
        self.logger = logging.getLogger(f"{__name__}.{name}")

    # ========== Abstract bắt buộc ==========

    @abstractmethod
    async def extract_async(self, **kwargs) -> ExtractionResult:
        """Async extraction method - lớp con phải implement."""
        pass

    @abstractmethod
    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        """Extract data từ nhiều branch (nếu có)."""
        pass

    @abstractmethod
    def close(self):
        """Dọn tài nguyên (session, connection, ...)."""
        pass