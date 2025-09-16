from typing import Dict, Any, List

# <<< SỬA LẠI CÁC DÒNG IMPORT DƯỚI ĐÂY >>>
from src.data_pipeline.extractors.pms.pms_extractor import PMSExtractor
from src.data_pipeline.extractors.pms.extraction_strategies import ExtractionStrategy
from src.data_pipeline.extractors.abstract_extractor import ExtractionResult

class DimensionExtractor(PMSExtractor):
    def __init__(self, endpoint: str, strategy: ExtractionStrategy):
        super().__init__()
        self.ENDPOINT = endpoint
        self.strategy = strategy
        self.name = f"DimensionExtractor-{endpoint}"
        self._data_cache = None

    async def _perform_extraction(self, session, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        if self._data_cache is None:
            self._data_cache = await self.strategy.fetch(self.client, session, self.ENDPOINT)
        return self._data_cache

    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        result = await self.extract_async(branch_id=0, **kwargs)
        target_branch_ids = kwargs.get("branch_ids") or list(self.TOKEN_BRANCH_MAP.keys())
        return {branch_id: result for branch_id in target_branch_ids}