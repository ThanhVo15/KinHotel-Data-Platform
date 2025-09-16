import aiohttp
import logging
from typing import Any, Dict, List

# --- Import các thành phần từ core và utils ---
from src.data_pipeline.core.pms.pms_extractor import PMSExtractor
from src.data_pipeline.core.abstract_extractor import ExtractionResult
from src.utils.date_params import DateWindow
from src.data_pipeline.core.clients.pms_client import PMSClient

logger = logging.getLogger(__name__)

class BookingExtractor(PMSExtractor):
    """Extractor chuyên dụng để lấy dữ liệu Booking."""
    ENDPOINT = "bookings"

    def __init__(self, client: PMSClient):
        super().__init__(client=client)

    async def _perform_extraction(self, session: aiohttp.ClientSession, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        """
        Thực hiện việc gọi API và phân trang để lấy dữ liệu thô.
        Lớp cha (PMSExtractor) sẽ truyền `params` từ `date_window` vào đây thông qua **kwargs.
        """
        base_params: Dict[str, Any] = kwargs.get("params", {})
        if "limit" not in base_params:
            base_params["limit"] = 100
            
        logger.info(f"Performing booking extraction for branch {branch_id} with params: {base_params}")
        return await self.client.paginate_json(session, self.ENDPOINT, base_params, limit_default=100)

    async def extract_async(self, *, branch_id: int, date_window: DateWindow, **kwargs) -> ExtractionResult:
        """
        Ghi đè (override) phương thức của lớp cha để thêm logging và truyền date_window.
        Đây là phương thức được gọi bởi template method trong lớp cha.
        """
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
        logger.info(
            f"🚀 Starting RAW extraction for {branch_name} ({date_window.field}): "
            f"{date_window.start.strftime('%Y-%m-%d')} -> {date_window.end.strftime('%Y-%m-%d')}"
        )
        
        # Tạo params từ date_window để truyền cho _perform_extraction
        api_params = date_window.as_api_params()
        
        # Gọi template method của lớp cha, truyền các tham số cần thiết vào kwargs.
        # Lớp cha sẽ xử lý việc gọi _perform_extraction, try/except, logging thành công/thất bại...
        return await super().extract_async(
            branch_id=branch_id,
            params=api_params, 
            update_from=date_window.start, # Truyền thêm thông tin này để lưu vào ExtractionResult
            update_to=date_window.end,
            **kwargs
        )