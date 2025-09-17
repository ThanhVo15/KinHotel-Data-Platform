import asyncio
import logging

from src.data_pipeline.core.clients.pms_client import PMSClient

from .booking_pipeline import run_booking_pipeline
from .dimensions_pipeline import run_dimensions_pipeline
from .odoo_pipeline import run_odoo_pipeline
from .room_lock_pipeline import run_room_lock_pipeline

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config: dict, report: dict):
        self.config = config
        self.report = report
        self.pms_client = None

    async def _get_pms_client(self):
        """Tạo PMS client chỉ khi cần thiết."""
        if not self.pms_client:
            pms_config = self.config.get('pms', {})
            base_url = (pms_config.get('base_url') or '').rstrip('/') + '/'
            self.pms_client = PMSClient(base_url)
        return self.pms_client

    async def run(self, pipeline_name: str):
        """
        Điểm điều phối chính, gọi runner tương ứng dựa trên tên.
        """
        if pipeline_name in ['booking', 'all']:
            client = await self._get_pms_client()
            await run_booking_pipeline(client, self.config, self.report)
        
        if pipeline_name in ['dimensions', 'all']:
            client = await self._get_pms_client()
            await run_dimensions_pipeline(client, self.config, self.report)

        if pipeline_name in ['room_lock', 'all']:
            client = await self._get_pms_client()
            await run_room_lock_pipeline(client, self.config, self.report)

        if pipeline_name in ['odoo', 'all']:
            # Odoo pipeline tự quản lý client của nó
            await run_odoo_pipeline(self.config, self.report)

    async def close_resources(self):
        """Đóng các kết nối dùng chung."""
        if self.pms_client:
            await self.pms_client.close()