# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\data_pipeline\pipelines\dimensions\extractor.py
import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from src.data_pipeline.core.abstract_extractor import AbstractExtractor, ExtractionResult
from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.config.pipeline_config import TOKEN_BRANCH_MAP

logger = logging.getLogger(__name__)

class DimensionExtractor(AbstractExtractor):
    """
    Extractor generic để lấy dữ liệu cho các bảng Dimension từ PMS.
    Nó đọc cấu hình và quyết định cách lấy dữ liệu (full load, phân trang, 
    dùng chung hay theo từng chi nhánh).
    """

    def __init__(self, config: Dict[str, Any], client: PMSClient):
        super().__init__(f"Dimension-{config['name']}")
        self.config = config
        self.client = client
        self.endpoint = config['endpoint']
        self._shared_data_cache: Optional[List[Dict[str, Any]]] = None

    async def _fetch_data_for_branch(self, session, branch_id: int) -> List[Dict[str, Any]]:
        """Lấy dữ liệu thô cho một chi nhánh."""
        logger.info(f"Fetching raw data for dimension '{self.name}' from branch {branch_id}...")

        base_params = self.config.get("base_params", {})

        if self.config['is_paginated']:
            return await self.client.paginate_json(session, self.endpoint, base_params=base_params) # <-- Dòng này đã được cập nhật
        else:
            full_url = f"{self.client.base_url.rstrip('/')}/{self.endpoint.lstrip('/')}"
            json_data, _ = await self.client.get_json(session, full_url, params={})
            if isinstance(json_data, dict) and 'data' in json_data:
                return json_data.get('data', [])
            return json_data if isinstance(json_data, list) else []

    async def extract_async(self, *, branch_id: int, **kwargs) -> ExtractionResult:
        """
        Gói gọn logic lấy dữ liệu cho một chi nhánh duy nhất.
        Sử dụng cache cho dữ liệu dùng chung (shared).
        """
        source_name = f"PMS:{self.endpoint}"
        branch_name = TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")

        try:
            # Nếu là dữ liệu dùng chung và đã có trong cache, trả về ngay
            if self.config['is_shared_across_branches'] and self._shared_data_cache is not None:
                logger.info(f"Using cached shared data for '{self.name}' for branch {branch_name}.")
                raw_records = self._shared_data_cache
            else:
                # Nếu không thì gọi API
                session = await self.client.get_session(branch_id)
                raw_records = await self._fetch_data_for_branch(session, branch_id)
                
                # Nếu là dữ liệu dùng chung, lưu vào cache cho các lần gọi sau
                if self.config['is_shared_across_branches']:
                    self._shared_data_cache = raw_records
            
            return ExtractionResult(
                data=raw_records,
                source=source_name,
                branch_id=branch_id,
                branch_name=branch_name,
                record_count=len(raw_records)
            )
        except Exception as e:
            logger.error(f"❌ Extraction failed for dimension '{self.name}' on {branch_name}: {e}", exc_info=True)
            return ExtractionResult(
                data=None, source=source_name, branch_id=branch_id,
                branch_name=branch_name, status="error", error=str(e)
            )

    async def extract_multi_branch(self, branch_ids: List[int], **kwargs) -> Dict[int, ExtractionResult]:
        """Điều phối việc lấy dữ liệu từ nhiều chi nhánh."""
        results: Dict[int, ExtractionResult] = {}
        
        if self.config['is_shared_across_branches']:
            # Nếu là shared data, chỉ cần gọi API một lần với branch_id đầu tiên
            first_branch_id = branch_ids[0]
            logger.info(f"Fetching SHARED dimension '{self.name}' using branch {first_branch_id} as representative.")
            result = await self.extract_async(branch_id=first_branch_id)
            # Sau đó nhân bản kết quả cho tất cả các branch khác
            for bid in branch_ids:
                if result.is_success:
                    results[bid] = ExtractionResult(
                        data=result.data, source=result.source, branch_id=bid,
                        branch_name=TOKEN_BRANCH_MAP.get(bid, f"Branch {bid}"),
                        record_count=result.record_count
                    )
                else:
                    results[bid] = result # Trả về lỗi nếu có
        else:
            # Nếu dữ liệu theo từng chi nhánh, gọi API song song cho mỗi chi nhánh
            logger.info(f"Fetching PER-BRANCH dimension '{self.name}' for {len(branch_ids)} branches.")
            tasks = [self.extract_async(branch_id=bid) for bid in branch_ids]
            task_results = await asyncio.gather(*tasks)
            for res in task_results:
                results[res.branch_id] = res
                
        return results
    
    async def close(self):
        # Vì client được truyền từ bên ngoài vào, chúng ta không đóng nó ở đây.
        # Orchestrator sẽ chịu trách nhiệm đóng client.
        pass