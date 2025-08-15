import aiohttp
import asyncio
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..abstract_extractor import AbstractExtractor, ExtractionResult

from ....utils.env_utils import get_config
from ....utils.token_manager import get_pms_token

logger = logging.getLogger(__name__)


class PMSExtractor(AbstractExtractor):
    """PMS Data Extractor with multi-branch support. Handles multiple endpoints."""

    PARSERS: Dict[str, Callable[[Any], Any]] = {}

    # --- Cấu hình cho logic retry ---
    RETRY_STATUS_CODES = (500, 502, 503, 504) # Các lỗi phía server đáng để thử lại
    MAX_RETRIES = 3
    RETRY_WAIT_MULTIPLIER = 1 
    RETRY_MAX_WAIT = 10

    # Branch mapping từ token suffix đến branch name
    TOKEN_BRANCH_MAP = {
        1:  "KIN HOTEL DONG DU",
        2:  "KIN HOTEL THI SACH EDITION", 
        3:  "KIN HOTEL THAI VAN LUNG",
        4:  "KIN WANDER TAN BINH, THE MOUNTAIN",
        5:  "KIN WANDER TAN QUY",
        6:  "KIN WANDER TAN PHONG, THE MOONAGE",
        7:  "KIN WANDER TRUNG SON",
        9:  "KIN HOTEL CENTRAL PARK",
        10: "KIN HOTEL LY TU TRONG"
    }

    def __init__(self):
        super().__init__("PMS")
        self.config = get_config()['pms']
        self.base_url = self.config['base_url']
        self._base_token: Optional[str] = None
        self._sessions: Dict[int, aiohttp.ClientSession] = {}

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_WAIT_MULTIPLIER, max=RETRY_MAX_WAIT),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying request (attempt {retry_state.attempt_number}/{MAX_RETRIES})... Waiting {retry_state.next_action.sleep:.2f}s."
        )
    )

    async def _make_request(self,
                            session: aiohttp.ClientSession,
                            url: str,
                            params: Dict) -> Any:
        self.logger.debug(f"GET {url} with params {params}")
        async with session.get(url, params=params) as repsone:
            if repsone.status in RETRY_STATUS_CODES:
                repsone.raise_for_status()
            return await repsone.json(), repsone.status

    def _get_base_token(self) -> str:
        """Get base token using /utils/token_manager.py"""
        if self._base_token is None:
            self._base_token = get_pms_token()
            logger.info(f"🔧 Extracted base token from: {self._base_token[:20]}...")
        return self._base_token
    
    async def close(self):
        self.logger.info("🧹 Closing all network sessions...")
        tasks = [s.close() for s in self._sessions.values() if not s.closed]
        await asyncio.gather(*tasks)
        self._sessions.clear()
        self.logger.info("🔒 All sessions closed.")

    def _get_branch_token(self, branch_id: int) -> str:
        """Create specific token for each branch"""
        base_token = self._get_base_token()
        branch_token = f"{base_token}|{branch_id}"
        logger.info(f"🔑 Branch {branch_id} token: {branch_token[:20]}...")
        return branch_token
    
    async def _get_session(self, 
                           branch_id: int) -> aiohttp.ClientSession:
        """Get or create aiohttp session for specific branch (unchanged, fixed typo 'sepcific' to 'specific')"""
        if branch_id not in self._sessions or self._sessions[branch_id].closed:
            token = self._get_branch_token(branch_id)
            headers = {
                "accept": "application/json, text/plain, */*",
                "authorization": f"Bearer {token}",
                "origin": "https://pms.kinliving.vn",
                "referer": "https://pms.kinliving.vn/",
                "user-agent": "Mozilla/5.0"
            }
            timeout = aiohttp.ClientTimeout(total = 30)
            self._sessions[branch_id] = aiohttp.ClientSession(
                headers = headers,
                timeout = timeout,
                connector = aiohttp.TCPConnector(limit=5)
            )
            logger.debug(f"🔗 Created session for branch {branch_id}")
        
        return self._sessions[branch_id]
    
    def _parse_response(self,
                        data: Any,
                        endpoint: str) -> Any:
        """Parse API response using PARSERS"""
        parser = self.PARSERS.get(endpoint, lambda d:d)
        try:
            parsed = parser(data)
            logger.debug(f"📦 Parsed {endpoint}: {len(parsed) if isinstance(parsed, list) else 1} records")
            return parsed
        except Exception as e:
            logger.error(f"❌ Parsing {endpoint} failed: {e}")
            return []
    
    async def extract_async(self, 
                            *, 
                            endpoint: str, 
                            branch_id: int, 
                            **kwargs) -> ExtractionResult:
        start_time = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Unknown Branch {branch_id}")
        
        params = kwargs.copy()
        if 'created_date_from' in params and isinstance(params['created_date_from'], datetime):
            params['created_date_from'] = params['created_date_from'].isoformat()
        if 'created_date_to' in params and isinstance(params['created_date_to'], datetime):
            params['created_date_to'] = params['created_date_to'].isoformat()
        
        url = f"{self.base_url}{endpoint}"
        try:
            session = await self._get_session(branch_id)
            self.logger.info(f"🔍 Extracting PMS data: {branch_name} - {endpoint}")
            
            data, status_code = await self._make_request(session, url, params=params)
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            record_count = len(data.get("data", []))
            self.logger.info(
                f"✅ Extracted {record_count} records from {branch_name} "
                f"for endpoint '{endpoint}' in {duration:.2f}s."
            )
            
            return ExtractionResult(
                data=data, source=f"PMS:{endpoint}", branch_id=branch_id,
                branch_name=branch_name, record_count=record_count,
                created_date_from=kwargs.get('created_date_from'),
                created_date_to=kwargs.get('created_date_to')
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.error(
                f"❌ Extraction failed for {branch_name} after {duration:.2f}s: {e}"
            )
            return ExtractionResult(
                data=None, source=f"PMS:{endpoint}", branch_id=branch_id,
                branch_name=branch_name, status="error", error=str(e),
                created_date_from=kwargs.get('created_date_from'),
                created_date_to=kwargs.get('created_date_to')
            )
    
    async def extract_multi_branch(self,
                            endpoint: str = None,
                            branch_ids: List[int] = None,
                            start_date: str = None,
                            end_date: str = None,
                            created_date_from: datetime = None,
                            created_date_to: datetime = None,
                            max_concurrent: int = 3,
                            **kwargs) -> Dict[int, ExtractionResult]:
        """Extract data from multiple branches concurrently"""
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        self.logger.info(f"🚀 Multi-branch extraction: {len(branch_ids)} branches")
        self.logger.info(f"📅 Period: {created_date_from} → {created_date_to}")
        self.logger.info(f"🔗 Endpoint: {endpoint}")

        start_time = datetime.now()

        # Create semaphore để limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)

        async def extract_single_branch(branch_id: int) -> ExtractionResult:
            async with semaphore:
                return await self.extract_async(
                    endpoint=endpoint,
                    branch_id=branch_id,
                    start_date=start_date,
                    end_date=end_date,
                    created_date_from = created_date_from,
                    created_date_to = created_date_to,
                    **kwargs
                )
        
        # Run all extractions concurrently
        tasks = [extract_single_branch(branch_id) for branch_id in branch_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        extraction_results = {}
        for branch_id, result in zip(branch_ids, results):
            if isinstance(result, Exception):
                self.logger.error(f"❌ Branch {branch_id} failed: {result}")
                extraction_results[branch_id] = ExtractionResult(
                    data=None,
                    source=f"PMS:{endpoint}",
                    branch_id=branch_id,
                    branch_name=self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}"),
                    extracted_at=datetime.now(),
                    status="error",
                    error=str(result),
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                extraction_results[branch_id] = result
        
        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        success_count = sum(1 for r in extraction_results.values() if r.is_success)
        total_records = sum(r.record_count for r in extraction_results.values() if r.is_success)
        
        self.logger.info(f"🎉 Multi-branch extraction completed:")
        self.logger.info(f"   ✅ Success: {success_count}/{len(branch_ids)} branches")
        self.logger.info(f"   📊 Records: {total_records} total")
        self.logger.info(f"   ⏱️  Duration: {duration:.2f}s")
        
        return extraction_results
    
    def validate_config(self):
        return bool(self.config and self.base_url) # True if set

if __name__ == '__main__':
    import asyncio
    from unittest.mock import patch, AsyncMock

    async def main_test():
        print("\n" + "="*50)
        print("🚀 Testing pms_extractor.py...")
        print("="*50)
        
        # Patch để giả lập các hàm phụ thuộc từ bên ngoài
        with patch('__main__.get_config', return_value={'pms': {'base_url': 'https://fake-api.com/api/'}}), \
             patch('__main__.get_pms_token', return_value='fake-token'):

            extractor = PMSExtractor()

            # Giả lập một response thành công từ API
            fake_response = ({"data": [{"id": 1}]}, 200)
            extractor._make_request = AsyncMock(return_value=fake_response)
            
            print("--- Testing Success Case ---")
            result = await extractor.extract_async(endpoint="test", branch_id=1)
            print(f"✅ Received result: {result.status}, {result.record_count} records")
            assert result.is_success and result.record_count == 1

            # Giả lập API trả về lỗi
            extractor._make_request.side_effect = Exception("Fake network error")
            print("\n--- Testing Failure Case ---")
            error_result = await extractor.extract_async(endpoint="test", branch_id=2)
            print(f"✅ Received result: {error_result.status}, Error: {error_result.error}")
            assert not error_result.is_success and "Fake network error" in error_result.error

            await extractor.close()

    asyncio.run(main_test())