import asyncio
import aiohttp
from src.utils.env_utils import get_config
from src.utils.token_manager import get_pms_token
import jwt
from datetime import datetime, timedelta

def create_branch_token(base_token: str, branch_id: int) -> str:
    cfg = get_config()
    secret = cfg["secret_key"]
    base_payload = jwt.decode(base_token, secret, algorithms=["HS256"])
    branch_payload = {**base_payload, "branch_id": branch_id}
    return jwt.encode(branch_payload, secret, algorithm="HS256")

async def test_bookings_with_different_params():
    """Test bookings endpoint with different parameter combinations"""
    cfg = get_config()
    
    # ✅ Fix: Check available keys and use correct case
    print("🔍 Available config keys:")
    for key in sorted(cfg.keys()):
        if "pms" in key.lower() or "base" in key.lower() or "url" in key.lower():
            print(f"  - {key}")
    
    # Try different possible key names
    base_url = None
    possible_keys = ["pms_base_url", "PMS_BASE_URL", "pms"]["base_url"]
    
    for key_path in ["pms_base_url", "PMS_BASE_URL"]:
        if key_path in cfg:
            base_url = cfg[key_path]
            print(f"✅ Found base URL with key: {key_path}")
            break
    
    # Try nested structure
    if not base_url and "pms" in cfg and isinstance(cfg["pms"], dict):
        if "base_url" in cfg["pms"]:
            base_url = cfg["pms"]["base_url"]
            print(f"✅ Found base URL in nested pms.base_url")
    
    if not base_url:
        print("❌ Could not find PMS base URL in config!")
        return None
        
    print(f"🌐 Using base URL: {base_url}")
    
    base_token = get_pms_token()
    branch_token = create_branch_token(base_token, 1)
    
    # Different date formats to try
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    
    param_combinations = [
        # No params (minimal request)
        {},
        
        # Different date field names and formats
        {
            "check_in_from": yesterday.strftime("%Y-%m-%d"),
            "check_in_to": now.strftime("%Y-%m-%d")
        },
        {
            "checkin_from": yesterday.strftime("%Y-%m-%d"),
            "checkin_to": now.strftime("%Y-%m-%d")
        },
        {
            "date_from": yesterday.strftime("%Y-%m-%d"),
            "date_to": now.strftime("%Y-%m-%d")
        },
        {
            "start_date": yesterday.strftime("%Y-%m-%d"),
            "end_date": now.strftime("%Y-%m-%d")
        },
        
        # With datetime format
        {
            "check_in_from": yesterday.strftime("%Y-%m-%d %H:%M:%S"),
            "check_in_to": now.strftime("%Y-%m-%d %H:%M:%S")
        },
        
        # With pagination only
        {
            "limit": 10,
            "page": 1
        }
    ]
    
    async with aiohttp.ClientSession() as session:
        headers = {
            "Authorization": f"Bearer {branch_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{base_url}bookings"
        
        for i, params in enumerate(param_combinations):
            print(f"\n🧪 Test {i+1}: {params}")
            
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    print(f"📊 Status: {resp.status}")
                    
                    if resp.status == 200:
                        print(f"✅ SUCCESS with params: {params}")
                        try:
                            data = await resp.json()
                            print(f"📋 Data preview: {str(data)[:200]}...")
                            return params  # Found working params!
                        except:
                            print("⚠️ Response not JSON")
                    else:
                        text = await resp.text()
                        print(f"❌ Error: {text[:100]}...")
                        
            except Exception as e:
                print(f"❌ Request failed: {e}")
    
    print("\n❌ No working parameter combinations found!")
    return None

if __name__ == "__main__":
    asyncio.run(test_bookings_with_different_params())