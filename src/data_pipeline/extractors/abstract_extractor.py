from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

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
    created_date_from: Optional[datetime] = None  
    created_date_to: Optional[datetime] = None
    
    @property
    def is_success(self) -> bool:
        return self.status == 'success' and self.error is None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'branch_id': self.branch_id,
            'branch_name': self.branch_name,
            'extracted_at': self.extracted_at.isoformat(),
            'record_count': self.record_count,
            'status': self.status,
            'error': self.error,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'created_date_from': self.created_date_from.isoformat() if self.created_date_from else None,
            'created_date_to': self.created_date_to.isoformat() if self.created_date_to else None,
        }

class AbstractExtractor(ABC):
    """Base class for all extractors. Defines the interface."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    async def extract_async(self, **kwargs) -> ExtractionResult:
        """Async extraction method - must implement"""
        pass

    def extract(self, **kwargs) -> ExtractionResult:
        """Sync wrapper cho extract_async"""
        return asyncio.run(self.extract_async(**kwargs))
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate extractor configuration"""
        pass

    @abstractmethod
    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        """Extract data từ tất cả branches"""
        pass

    def get_status(self) -> Dict[str, Any]:
        """Get extractor status"""
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "config_valid": self.validate_config()
        }
    
    @abstractmethod
    def close(self):
        """Method to clean up resources."""
        pass





if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 Testing abstract_extractor.py...")
    print("="*50)

    print("INFO: Testing the ExtractionResult dataclass.\n")

    # 1. Test trường hợp thành công
    print("--- Testing Success Case ---")
    success_result = ExtractionResult(
        data=[{"id": 1}],
        source="test_source",
        record_count=1
    )
    print(f"Result object: {success_result}")
    assert success_result.is_success is True
    assert success_result.extracted_at.tzinfo is not None # Phải là timezone-aware
    print("✅ Success case verified.\n")

    # 2. Test trường hợp thất bại
    print("--- Testing Failure Case ---")
    error_result = ExtractionResult(
        data=None,
        source="test_source",
        status="error",
        error="Test error."
    )
    print(f"Result object: {error_result}")
    assert error_result.is_success is False
    print("✅ Failure case verified.")