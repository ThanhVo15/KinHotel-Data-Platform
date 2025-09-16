import aiohttp
from typing import Any, Dict, List

from src.data_pipeline.core.clients.pms_client import PMSClient
from src.data_pipeline.core.pms.pms_extractor import PMSExtractor

class RoomLockExtractor(PMSExtractor):
    """Extractor chuyên dụng để lấy dữ liệu Room Lock."""
    ENDPOINT = "room-lock"

    def __init__(self, client: PMSClient):
        super().__init__(client=client)

    async def _perform_extraction(self, session: aiohttp.ClientSession, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        """
        Thực hiện gọi API. Endpoint này không phân trang,
        nhưng trả về dữ liệu trong key "data".
        """
        params = kwargs.get("params", {})
        # Thêm tham số mặc định nếu API yêu cầu
        params.setdefault("active", "report")
        
        full_url = f"{self.client.base_url.rstrip('/')}/{self.ENDPOINT.lstrip('/')}"
        json_data, _ = await self.client.get_json(session, full_url, params=params)
        
        # Dữ liệu nằm trong key 'data'
        return json_data.get("data", []) if json_data else []