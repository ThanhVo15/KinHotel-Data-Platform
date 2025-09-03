"""
Token Manager - Quản lý tokens cho PMS & Odoo với parallel processing
Author: Thanh Vo
"""

import datetime
import requests
import logging
import json
import re
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .env_utils import get_config
from .logger import setup_logging

# ============================================================================
# SETUP & MODELS
# ============================================================================
setup_logging('INFO')
logger = logging.getLogger(__name__)

@dataclass
class TokenInfo:
    access_token: str
    expires_at: Optional[datetime.datetime] = None
    service: str = "unknown"
    
    def is_expired(self, buffer_minutes: int = 30) -> bool:
        if not self.expires_at:
            return False
        buffer = datetime.timedelta(minutes=buffer_minutes)
        return datetime.datetime.utcnow() >= (self.expires_at - buffer)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'access_token': self.access_token,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'service': self.service
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TokenInfo':
        expires_at = None
        if data.get('expires_at'):
            expires_at = datetime.datetime.fromisoformat(data['expires_at'])
        return cls(data['access_token'], expires_at, data.get('service', 'unknown'))

# ============================================================================
# TOKEN MANAGER
# ============================================================================
class TokenManager:
    def __init__(self, cache_file: str = None):
        self.config = get_config()
        self.token_cache: Dict[str, TokenInfo] = {}
        
        if cache_file is None:
            # Tạo cache folder relative to project root
            cache_dir = Path(__file__).parent.parent / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_file = cache_dir / "tokens_cache.json"
        else:
            self.cache_file = Path(cache_file)
            
        self._load_cache()
        logger.info(f"🔧 TokenManager initialized, cache: {self.cache_file}")
    
    def _load_cache(self):
        """Load cached tokens"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for service, token_data in data.items():
                        try:
                            self.token_cache[service] = TokenInfo.from_dict(token_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Invalid cached token for {service}: {e}")
                logger.info(f"✅ Loaded {len(self.token_cache)} cached tokens")
        except Exception as e:
            logger.error(f"❌ Cache load failed: {e}")
    
    def _save_cache(self):
        """Save tokens to cache"""
        try:
            cache_data = {service: token.to_dict() for service, token in self.token_cache.items()}
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(cache_data)} tokens")
        except Exception as e:
            logger.error(f"❌ Cache save failed: {e}")
    
    def _get_cached_token(self, service: str) -> Optional[TokenInfo]:
        """Get valid cached token"""
        if service not in self.token_cache:
            return None
        token = self.token_cache[service]
        if token.is_expired():
            logger.info(f"⏰ {service} token expired")
            del self.token_cache[service]
            return None
        logger.info(f"🔄 Using cached {service} token")
        return token
    
    def _fetch_pms_token(self) -> TokenInfo:
        """Fetch PMS token"""
        pms_config = self.config['pms']
        login_url = f"{pms_config['base_url']}auth/login"
        
        response = requests.post(
            login_url,
            json={"email": pms_config['email'], "password": pms_config['password']},
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        # Extract token from possible paths
        token = (data.get("data", {}).get("access_token") or 
                data.get("access_token") or 
                data.get("token") or 
                data.get("data", {}).get("token"))
        
        if not token:
            raise RuntimeError(f"No token in PMS response: {list(data.keys())}")
        
        def _mask(t: str) -> str: return (t[:4] + "…" + t[-4:]) if t and len(t) > 8 else "***"
        logger.info(f"✅ PMS token: {_mask(token)}")

        return TokenInfo(
            access_token=token,
            expires_at=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
            service="pms"
        )
    
    def _fetch_odoo_token(self) -> TokenInfo:
        """Fetch Odoo token with fallback to session_id"""
        odoo_config = self.config['odoo']
        session = requests.Session()
        session.headers.update({
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        
        # Step 1: Login and get session
        login_url = f"{odoo_config['url']}/web/login"
        resp = session.get(login_url, timeout=30)
        resp.raise_for_status()
        
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', resp.text)
        if not csrf_match:
            raise RuntimeError("CSRF token not found")
        csrf_token = csrf_match.group(1)
        
        login_resp = session.post(
            login_url,
            data={
                "login": odoo_config['email'],
                "password": odoo_config['password'],
                "csrf_token": csrf_token,
                "redirect": "/web"
            },
            allow_redirects=False,
            timeout=30
        )
        
        if login_resp.status_code not in (302, 303, 307):
            raise RuntimeError(f"Odoo login failed: {login_resp.status_code}")
        
        session_id = session.cookies.get("session_id")
        if not session_id:
            raise RuntimeError("No session_id cookie")
        
        # Step 2: Try to get external token, fallback to session_id
        external_token = self._get_external_token(session, session_id)
        if not external_token:
            logger.warning("⚠️ Using session_id as token fallback")
            external_token = session_id
        
        logger.info(f"✅ Odoo token: {external_token[:20]}...")
        return TokenInfo(
            access_token=external_token,
            expires_at=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
            service="odoo"
        )
    
    def _get_external_token(self, session: requests.Session, session_id: str) -> Optional[str]:
        """Try to get external access token from Odoo config"""
        try:
            config_url = f"{self.config['odoo']['url']}/web/dataset/call_kw"
            payload = {
                'jsonrpc': '2.0',
                'method': 'call',
                'params': {
                    'model': 'ir.config_parameter',
                    'method': 'get_param',
                    'args': ['hotel_management_system.auth_external_access'],
                    'kwargs': {}
                },
                'id': 1
            }
            
            response = session.post(
                config_url, 
                json=payload,
                headers={'Cookie': f'session_id={session_id}'},
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if 'error' not in result and result.get('result'):
                logger.info("✅ External token found")
                return result['result']
            
        except Exception as e:
            logger.warning(f"External token failed: {e}")
        
        return None
    
    def get_token(self, service: str, force_refresh: bool = False) -> str:
        """Get token for service"""
        # Check cache first
        if not force_refresh:
            cached = self._get_cached_token(service)
            if cached:
                return cached.access_token
        
        logger.info(f"🔑 Fetching new {service} token...")
        
        # Fetch new token
        if service == "pms":
            token_info = self._fetch_pms_token()
        elif service == "odoo":
            token_info = self._fetch_odoo_token()
        else:
            raise ValueError(f"Unsupported service: {service}")
        
        # Cache and return
        self.token_cache[service] = token_info
        self._save_cache()
        return token_info.access_token
    
    def get_all_tokens(self, force_refresh: bool = False) -> Dict[str, str]:
        """Get all tokens in parallel"""
        services = ["pms", "odoo"]
        
        # Check cache first
        if not force_refresh:
            cached_tokens = {}
            for service in services:
                cached = self._get_cached_token(service)
                if cached:
                    cached_tokens[service] = cached.access_token
            
            if len(cached_tokens) == len(services):
                logger.info("🔄 Using all cached tokens")
                return cached_tokens
        
        logger.info("🚀 Fetching tokens in parallel...")
        start_time = datetime.datetime.now()
        
        tokens = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_service = {
                executor.submit(self.get_token, service, force_refresh): service 
                for service in services
            }
            
            for future in as_completed(future_to_service):
                service = future_to_service[future]
                try:
                    tokens[service] = future.result()
                    logger.info(f"✅ {service} ready")
                except Exception as e:
                    logger.error(f"❌ {service} failed: {e}")
        
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(f"🎉 Got {len(tokens)}/{len(services)} tokens in {duration:.2f}s")
        return tokens
    
    def clear_cache(self):
        """Clear all cached tokens"""
        self.token_cache.clear()
        if self.cache_file.exists():
            self.cache_file.unlink()
        logger.info("🧹 Cache cleared")

# ============================================================================
# GLOBAL INSTANCE & CONVENIENCE FUNCTIONS
# ============================================================================
token_manager = TokenManager()

def get_pms_token(force_refresh: bool = False) -> str:
    """Get PMS token"""
    return token_manager.get_token("pms", force_refresh)

def get_odoo_token(force_refresh: bool = False) -> str:
    """Get Odoo token"""
    return token_manager.get_token("odoo", force_refresh)

def get_all_tokens(force_refresh: bool = False) -> Dict[str, str]:
    """Get all tokens in parallel"""
    return token_manager.get_all_tokens(force_refresh)

# ============================================================================
# TESTING
# ============================================================================
# Sửa dòng 316-320 trong phần TESTING
if __name__ == "__main__":
    try:
        logger.info("🧪 Testing Token Manager...")
        
        # 🔧 SỬA: Test individual token fetching
        pms_token = get_pms_token()
        logger.info(f"📱 PMS: {pms_token[:20]}...")
        
        odoo_token = get_odoo_token()
        logger.info(f"📱 Odoo: {odoo_token[:20]}...")
        
        # Test parallel token fetching
        all_tokens = get_all_tokens()
        logger.info(f"🎉 Got all tokens: {list(all_tokens.keys())}")

        logger.info("🎉 All tests passed!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")