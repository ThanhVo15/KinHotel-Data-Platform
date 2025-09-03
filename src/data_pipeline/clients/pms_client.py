# src/data_pipeline/clients/pms_client.py
import aiohttp
import asyncio
import logging
import os
import json
import math
import random
from typing import Any, Dict, List, Optional, Tuple

from ...utils.token_manager import get_pms_token

logger = logging.getLogger(__name__)

def _parse_codes(s: Optional[str], default: List[int]) -> List[int]:
    if not s:
        return default
    try:
        return [int(x.strip()) for x in s.split(",") if x.strip()]
    except Exception:
        return default

class PMSClient:
    """
    Lớp client thuần HTTP cho PMS:
      - Quản lý base/branch token
      - Tạo & cache session theo branch (cookie_jar=Dummy tránh WAF)
      - Gọi GET JSON với retry (đọc cấu hình từ .env)
      - Phân trang chuẩn theo 'pagination'
    """

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/") + "/"
        self._base_token: Optional[str] = None
        self._sessions: Dict[int, aiohttp.ClientSession] = {}

        # Retry config từ .env (không hardcode)
        self.retry_max_attempts: int = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
        self.retry_wait_multiplier: float = float(os.getenv("RETRY_WAIT_MULTIPLIER", "1"))
        self.retry_max_wait: float = float(os.getenv("RETRY_MAX_WAIT", "10"))
        self.retry_status_codes: List[int] = _parse_codes(
            os.getenv("RETRY_STATUS_CODES"),
            [429, 500, 502, 503, 504],
        )
        self.non_retry_status_codes: List[int] = _parse_codes(
            os.getenv("NON_RETRY_STATUS_CODES"),
            [401, 403],
        )
        logger.info(
            f"[PMSClient retry] attempts={self.retry_max_attempts}, mult={self.retry_wait_multiplier}, "
            f"max_wait={self.retry_max_wait}, retry_codes={self.retry_status_codes}, "
            f"non_retry_codes={self.non_retry_status_codes}"
        )

    # ---------------- Token ----------------
    def _get_base_token(self) -> str:
        if self._base_token is None:
            self._base_token = get_pms_token()
            logger.info(f"🔧 Extracted base token from: {self._base_token[:20]}...")
        return self._base_token

    def get_branch_token(self, branch_id: int) -> str:
        return f"{self._get_base_token()}|{branch_id}"

    # ---------------- Session ----------------
    async def get_session(self, branch_id: int) -> aiohttp.ClientSession:
        if branch_id not in self._sessions or self._sessions[branch_id].closed:
            token = self.get_branch_token(branch_id)
            headers = {
                "accept": "application/json, text/plain, */*",
                "authorization": f"Bearer {token}",
                "user-agent": "Mozilla/5.0",
                "origin": "https://pms.kinliving.vn",
                "referer": "https://pms.kinliving.vn/",
                "accept-language": "vi,en-US;q=0.9,en;q=0.8",
                "x-requested-with": "XMLHttpRequest",
            }
            timeout = aiohttp.ClientTimeout(total=60)
            self._sessions[branch_id] = aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=aiohttp.TCPConnector(limit=5),
                cookie_jar=aiohttp.DummyCookieJar(),  # KHÔNG dùng cookie dính session cũ
            )
            logger.debug(f"🔗 Created session for branch {branch_id}")
        return self._sessions[branch_id]

    async def close(self):
        logger.info("🧹 Closing PMSClient sessions...")
        tasks = [s.close() for s in self._sessions.values() if not s.closed]
        if tasks:
            await asyncio.gather(*tasks)
        self._sessions.clear()
        logger.info("🔒 PMSClient sessions closed.")

    # ---------------- HTTP GET + retry ----------------
    async def get_json(self, session: aiohttp.ClientSession, url: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """
        Manual retry:
          - retry 5xx/429 (tôn trọng Retry-After)
          - 403 HTML (WAF) → retry nhẹ
          - 401/403 JSON (ACCESS_ERROR) → không retry
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                async with session.get(url, params=params) as resp:
                    text = await resp.text()

                    # 401/403 JSON: dừng ngay
                    if resp.status in self.non_retry_status_codes and text.strip().startswith("{"):
                        logger.error(f"Permission error {resp.status} for {url} params={params} body={text[:300]}")
                        raise PermissionError(f"HTTP {resp.status} for {url}")

                    # 403 HTML: retry nhẹ
                    if resp.status == 403 and text.lstrip().startswith("<!DOCTYPE"):
                        if attempt >= self.retry_max_attempts:
                            raise PermissionError("403 (WAF) after retries.")
                        sleep_s = min(2 * attempt, self.retry_max_wait)
                        logger.warning(f"403 HTML (WAF) → sleep {sleep_s}s rồi retry (attempt {attempt})")
                        await asyncio.sleep(sleep_s)
                        continue

                    # 5xx/429: retry
                    if resp.status in self.retry_status_codes:
                        if attempt >= self.retry_max_attempts:
                            raise RuntimeError(f"HTTP {resp.status} after retries: {text[:200]}")
                        ra = resp.headers.get("Retry-After")
                        if ra:
                            try:
                                sleep_s = int(ra)
                            except Exception:
                                sleep_s = min(self.retry_wait_multiplier * (2 ** (attempt - 1)), self.retry_max_wait)
                        else:
                            sleep_s = min(self.retry_wait_multiplier * (2 ** (attempt - 1)), self.retry_max_wait)
                        logger.warning(f"Server {resp.status} (attempt={attempt}) → sleep {sleep_s}s; body={text[:200]}")
                        await asyncio.sleep(sleep_s)
                        continue

                    # parse JSON
                    try:
                        return json.loads(text), resp.status
                    except Exception:
                        logger.error(f"Non-JSON response (status={resp.status}) for {url}: {text[:200]}")
                        raise RuntimeError(f"Non-JSON response {resp.status}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= self.retry_max_attempts:
                    raise
                sleep_s = min(self.retry_wait_multiplier * (2 ** (attempt - 1)), self.retry_max_wait)
                logger.warning(f"Network error {e}. Retry in {sleep_s:.2f}s (attempt {attempt})")
                await asyncio.sleep(sleep_s)

    # ---------------- Paginate chuẩn ----------------
    async def paginate_json(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        base_params: Dict[str, Any],
        *,
        limit_default: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Phân trang theo schema:
          { data: [...], pagination: { total, limit, page } }
        - Không gửi 'branch_id' lên server
        - Jitter nhẹ chống WAF
        """
        url = f"{self.base_url}{endpoint.strip('/')}"
        all_records: List[Dict[str, Any]] = []
        page = 1
        limit = int(base_params.get("limit", limit_default))

        while True:
            params = {**base_params, "page": page, "limit": limit}
            params.pop("branch_id", None)
            logger.info(f"🔍 Fetching page {page} …")
            data, status_code = await self.get_json(session, url, params)

            raw = (data or {}).get("data", [])
            if not raw:
                logger.info("✅ No more records. Pagination complete.")
                break

            all_records.extend(raw)

            pag = (data or {}).get("pagination") or {}
            try:
                total = int(pag.get("total", len(raw)))
                per = int(pag.get("limit", limit))
                cur = int(pag.get("page", page))
                max_pages = max(1, math.ceil(total / max(1, per)))
                logger.info(f"📄 Pagination: page {cur}/{max_pages} (total={total}, per={per})")
                if cur >= max_pages:
                    logger.info(f"✅ Final page ({cur}/{max_pages}).")
                    break
            except Exception:
                if len(raw) < limit:
                    logger.info(f"✅ Final page detected (got {len(raw)} < limit {limit}).")
                    break

            page += 1
            await asyncio.sleep(0.4 + random.random() * 0.6)

        return all_records
