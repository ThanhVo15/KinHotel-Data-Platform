from .logger import setup_logging
import logging
from dotenv import load_dotenv
import os

# Setup custom logger
setup_logging('INFO')
logger = logging.getLogger(__name__)

# Load env
load_dotenv()

# Define required environment variables
REQUIRED_ENV_VARS = {
    'ODOO_EMAIL',
    'ODOO_PASSWORD', 
    'ODOO_URL',

    'PMS_EMAIL',
    'PMS_PASSWORD',
    'PMS_BASE_URL',

    'EMAIL_SENDER',
    'EMAIL_PASSWORD',
    'EMAIL_RECIPIENT',

    'GDRIVE_FOLDER_ID',

    'MAX_WORKERS',
    'RETRY_LIMIT',
    'RETRY_DELAY',

    'ALGORITHM',
    'SECRET_KEY'
}

def validate_env_value(key: str, value: str) -> bool:
    """
    Validate environment variable value based on key type.
    Returns True if valid, False otherwise.
    """
    if not value or value.isspace():
        return False
    
    # Numeric Validation
    if key in ['MAX_WORKERS', 'RETRY_LIMIT', 'RETRY_DELAY']:
        try:
            int(value)
        except ValueError:
            logger.error(f"{key} should be a number")
            return False
        
    return True

def get_env_config() -> dict:
    """
    Get only required environment variables with validation.
    Returns dict of validated env vars.
    """

    env_dict = {}

    for key in REQUIRED_ENV_VARS:
        value = os.getenv(key)
        if value is not None:
            env_dict[key] = value
        else:
            logger.warning(f"Environment varaiable {key} is not set")
    
    return env_dict

def check_env_vars() -> bool:
    """
    Check and validate all required environment variables.
    Returns True if all valid, False if any missing/invalid.
    """
    logger.info(f"🔍 Checking environment variables...")

    env_dict = get_env_config()
    all_valid = True

    if env_dict is None:
        logger.error(f"❌ No environment variables found!")
        return False
    
    for key in REQUIRED_ENV_VARS:
        value = env_dict.get(key)

        if value is None:
            logger.error(f"❌ {key}: [MISSING]")
            return False
        elif validate_env_value(key, value):
            display_value = '***HIDDEN***' if 'PASSWORD' in key else value[:5] + '...' if len(value) > 5 else value
            logger.info(f"✅ {key}: {display_value} [PASS]")
        else:
            logger.error(f"❌ {key}: [INVALID]")
            all_valid = False

    if all_valid:
            logger.info(f"🎉 All environment variables are valid!")
    else:
        logger.error(f"💥 Some environment variables are missing or invalid!")
    
    return all_valid

def get_config() -> dict:
    """
    Get validated configuration for production use.
    Raises ValueError if any required env var is missing/invalid.
    """
    if not check_env_vars():
        raise ValueError("Environment configuration is invalid!")
    
    return {
        # Odoo config
        'odoo': {
            'email': os.getenv('ODOO_EMAIL'),
            'password': os.getenv('ODOO_PASSWORD'),
            'url': os.getenv('ODOO_URL')
        },
        # PMS config  
        'pms': {
            'email': os.getenv('PMS_EMAIL'),
            'password': os.getenv('PMS_PASSWORD'),
            'base_url': os.getenv('PMS_BASE_URL')
        },
        # Email config
        'email': {
            'sender': os.getenv('EMAIL_SENDER'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'recipient': os.getenv('EMAIL_RECIPIENT')
        },
        # Google Drive config
        'gdrive': {
            'folder_id': os.getenv('GDRIVE_FOLDER_ID')
        },
        # Performance config
        'performance': {
            'max_workers': int(os.getenv('MAX_WORKERS', 5)),
            'retry_limit': int(os.getenv('RETRY_LIMIT', 3)),
            'retry_delay': int(os.getenv('RETRY_DELAY', 5))
        },
        'token': {
            'secret_key': os.getenv('SECRET_KEY'),
            'algorithm': os.getenv('ALGORITHM')
        }
    }

if __name__ == "__main__":
    try:
        config = get_config()
        logger.info("🚀 Configuration loaded successfully!")
        
        # Test access
        logger.info(f"📧 Odoo Email: {config['token']['secret_key']}")
        logger.info(f"🔧 Max Workers: {config['token']['algorithm']}")
        
    except ValueError as e:
        logger.error(f"❌ Configuration failed: {e}")