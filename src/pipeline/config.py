import logging
import os 
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from colorama import Fore, init

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    handlers= [
        logging.FileHandler("env_check.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---- LOAD ENV ----
load_dotenv()
init(autoreset = True)

# ---- CONFIG ----
def get_env_var():
    """
    Automatically collect all environment variable key-value pairs from the .env file.
    Returns a dict: {key: value}
    """
    env_path = find_dotenv()
    env_dict = {}

    if env_path:
        with open(env_path, "r", encoding= 'utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_dict[key.strip()] = value.strip()
    else:
        logger.warning("Cannot find the .env file path.")
    
    return env_dict


def check_env_vars():
    """
    Check whether all required environment variables are set.
    Print [PASS] in green if set, [FAIL] in red if missing.
    Also log the result.
    """
    env_vars = get_env_var()

    if not env_vars:
        logger.error(f"{Fore.RED}env_dict is empty")
        return

    for key, expected_value in env_vars.items():
        actual_value = os.getenv(key)
        if actual_value is not None:
            logger.info(f"{Fore.GREEN}{key}: {'*'*8} [PASS]")
        else:
            logger.error(f"{Fore.RED}{key}: [FAIL]")

if __name__ == "__main__":
    check_env_vars()