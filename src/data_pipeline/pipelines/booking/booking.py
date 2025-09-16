# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\data_pipeline\extractors\pms\booking.py
import aiohttp
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from ...core.pms.pms_extractor import PMSExtractor
from ....core.abstract_extractor import ExtractionResult
from ....utils.date_params import DateWindow

logger = logging.getLogger(__name__)

class BookingListExtractor(PMSExtractor):
    ENDPOINT = "bookings"
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()

    async def _perform_extraction(self, session: aiohttp.ClientSession, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        """
        Thực hiện việc gọi API và phân trang để lấy dữ liệu thô.
        """
        base_params: Dict[str, Any] = kwargs.get("params", {})
        if "limit" not in base_params:
            base_params["limit"] = 100
        return await self.client.paginate_json(session, self.ENDPOINT, base_params, limit_default=100)


    async def extract_async(self, *, branch_id: int, date_window: DateWindow, **kwargs) -> ExtractionResult:
        """
        Hàm này giờ chỉ thiết lập tham số và gọi logic của lớp cha.
        Không còn flatten, không còn metadata.
        """
        start_ts = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")

        try:
            params = date_window.as_api_params()
            params.setdefault("limit", 100)
            
            self.logger.info(
                f"🚀 Starting RAW extraction for {branch_name} ({date_window.field}): "
                f"{date_window.start.strftime('%Y-%m-%d')} -> {date_window.end.strftime('%Y-%m-%d')}"
            )

            session = await self.client.get_session(branch_id)
            raw_records = await self._perform_extraction(session, branch_id, params=params)

            self.logger.info(f"✅ Extracted {len(raw_records)} raw records for {branch_name}.")
            
            return ExtractionResult(
                data=raw_records,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                record_count=len(raw_records),
                update_from=date_window.start,
                update_to=date_window.end,
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self.logger.error(f"❌ PMS {branch_name} failed after {duration:.2f}s: {e}")
            
            # Ghi metadata lỗi (nếu có thể)
            if 'date_window' in locals():
                self.metadata_manager.save_extract_metadata(
                    dataset=self.ENDPOINT, branch_id=branch_id, extract_time=start_ts,
                    window_start=date_window.start, window_end=date_window.end,
                    record_count=0, status="error", error=str(e)
                )
            
            return ExtractionResult(
                data=None, source=f"PMS:{self.ENDPOINT}", branch_id=branch_id,
                branch_name=branch_name, status="error", error=str(e)
            )

    async def extract_bookings_incrementally(self, **kwargs) -> Dict[int, ExtractionResult]:
        """Tận dụng extract_incremental từ lớp cha."""
        return await self.extract_incremental(endpoint=self.ENDPOINT, **kwargs)
    
    async def extract_multi_branch(self, **kwargs) -> Dict[int, ExtractionResult]:
        """Triển khai abstract method."""
        return await self.extract_bookings_incrementally(**kwargs)