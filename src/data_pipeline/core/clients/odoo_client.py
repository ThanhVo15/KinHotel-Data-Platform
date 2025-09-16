import aiohttp
import asyncio
import logging
import re
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)

class OdooClient:
    """
    Client bất đồng bộ để tương tác với Odoo,
    bao gồm đăng nhập và export CSV.
    """
    def __init__(self, odoo_url: str, email: str, password: str):
        self.base_url = odoo_url.rstrip('/')
        self.login_url = f"{self.base_url}/web/login"
        self.export_url = f"{self.base_url}/web/export/csv"
        self.email = email
        self.password = password
        
        self.session = aiohttp.ClientSession(
            headers={ "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" }
        )
        self.csrf_token = None
        logger.info(f"OdooClient initialized for URL: {self.base_url}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("OdooClient session closed.")

    async def login(self) -> bool:
        try:
            logger.info("Odoo Login Step 1: Fetching login page...")
            async with self.session.get(self.login_url) as resp:
                if resp.status != 200: return False
                html = await resp.text()
                match = re.search(r'name="csrf_token"\s*value="([^"]+)"', html)
                if not match: return False
                csrf_token = match.group(1)

            logger.info("Odoo Login Step 2: Submitting login credentials...")
            login_data = {"csrf_token": csrf_token, "login": self.email, "password": self.password}
            async with self.session.post(self.login_url, data=login_data, allow_redirects=False) as resp:
                if resp.status != 303: return False

            session_id = self.session.cookie_jar.filter_cookies(self.base_url).get("session_id")
            if not session_id: return False

            logger.info(f"✅ Odoo login successful. Session ID: ...{session_id.value[-6:]}")
            return True
        except Exception as e:
            logger.exception(f"An exception occurred during Odoo login: {e}")
            return False

    async def export_csv(self, model: str, fields: list) -> str | None:
        try:
            logger.info("Odoo Export Step 1: Fetching main page for fresh CSRF token...")
            async with self.session.get(f"{self.base_url}/web") as resp:
                html = await resp.text()
                
                patterns = [
                    # Ưu tiên tìm trong JS object trước
                    r'csrf_token:\s*"([^"]+)"',
                    # Các trường hợp khác
                    r'"csrf_token"\s*:\s*"([^"]+)"',
                    r'<input[^>]*name="csrf_token"[^>]*value="([^"]+)"',
                    r'window.csrf_token\s*=\s*"([^"]+)"'
                ]
                
                token_found = False
                for i, pattern in enumerate(patterns, 1):
                    match = re.search(pattern, html)
                    if match:
                        self.csrf_token = match.group(1)
                        logger.info(f"✅ Found CSRF token using pattern #{i}.")
                        token_found = True
                        break
                
                if not token_found:
                    logger.error("Could not find CSRF token on main web page with any pattern.")
                    debug_path = "odoo_debug_page.html"
                    with open(debug_path, "w", encoding="utf-8") as f: f.write(html)
                    logger.info(f"!!! ĐÃ LƯU NỘI DUNG TRANG WEB VÀO FILE: {debug_path} ĐỂ KIỂM TRA !!!")
                    return None
            
            logger.info(f"Odoo Export Step 2: Preparing export payload for model '{model}'...")
            payload_data = {
                "import_compat": False,
                "context": {"lang": "en_US", "tz": "Asia/Saigon", "uid": 505, "allowed_company_ids": [6]},
                "domain": [], "fields": fields, "groupby": [], "ids": False, "model": model
            }

            form = aiohttp.FormData()
            form.add_field("data", json.dumps(payload_data))
            form.add_field("token", "dummy-because-api-expects-one")
            form.add_field("csrf_token", self.csrf_token)

            logger.info("Odoo Export Step 3: Sending export request...")
            async with self.session.post(self.export_url, data=form) as resp:
                if resp.status != 200:
                    logger.error(f"Odoo export failed. Status: {resp.status}, Reason: {await resp.text()}")
                    return None
                
                csv_data = await resp.text()
                logger.info(f"✅ Odoo export successful. Received {len(csv_data)} bytes of CSV data.")
                return csv_data

        except Exception as e:
            logger.exception(f"An exception occurred during Odoo export: {e}")
            return None