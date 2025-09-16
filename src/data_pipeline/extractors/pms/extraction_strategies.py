from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from src.utils.state_manager import load_last_run_timestamp
from src.utils.date_params import DateWindow
import math, random, asyncio

class ExtractionStrategy(ABC):
    @abstractmethod
    async def fetch(self, client, session, endpoint) -> List[Dict[str, Any]]:
        pass

class FullLoadStrategy(ExtractionStrategy):
    async def fetch(self, client, session, endpoint) -> List[Dict[str, Any]]:
        full_url = f"{client.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        json_data, _ = await client.get_json(session, full_url, params={})
        if isinstance(json_data, dict) and 'data' in json_data:
            return json_data.get('data', [])
        return json_data if isinstance(json_data, list) else []

class PaginatedLoadStrategy(ExtractionStrategy):
    async def fetch(self, client, session, endpoint) -> List[Dict[str, Any]]:
        return await client.paginate_json(session, endpoint, base_params={"limit": 1000})

class IncrementalLoadStrategy(ExtractionStrategy):
    def __init__(self, field: str, lookback_days: int = 7):
        self.field = field
        self.lookback_days = lookback_days
        
    async def fetch(self, client, session, endpoint) -> List[Dict[str, Any]]:
        source_key = f"{endpoint}:{self.field}"
        last_run_utc = load_last_run_timestamp(source=source_key, branch_id=0)
        start_utc = (last_run_utc - timedelta(minutes=15)) if last_run_utc else (datetime.now(timezone.utc) - timedelta(days=self.lookback_days))
        end_utc = datetime.now(timezone.utc)
        date_window = DateWindow.from_utc(start_utc, end_utc, field=self.field)
        params = date_window.as_api_params()
        return await client.paginate_json(session, endpoint, base_params=params)