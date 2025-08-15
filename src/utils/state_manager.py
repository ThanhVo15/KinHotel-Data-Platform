"""This module centralizes the logic for persisting the pipeline's state.
By saving the last successful timestamp to a file, we can avoid re-fetching
the same data, making subsequent runs faster and more efficient."""

# Output: UTC nha !!!

import json
import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_FILE = Path("src/cache/extraction_state.json")

def load_last_run_timestamp(source: str, 
                            branch_id: int) -> Optional[datetime]: 
    """
    Loads the last successful run timestamp for a specific source and branch.
    Returns a timezone-aware datetime object or None if not found.
    """

    if not CACHE_FILE.exists():
        return None
    
    try:
        with open(CACHE_FILE, "r") as f:
            state = json.load(f)
        
        timestamp_str = state.get(source, {}).get(str(branch_id))
        if timestamp_str:
            dt = datetime.fromisoformat(timestamp_str)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo= timezone.utc)
            logger.info(f"💾 Loaded last run for {source}/{branch_id}: {dt.isoformat()}")
            return dt
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"❌ Failed to load state from {CACHE_FILE}: {e}")

    return None

def save_last_run_timestamp(source: str,
                            branch_id: int,
                            timestamp: datetime):
    """
    Saves the successful run timestamp for a specific source and branch.
    Timestamps are saved in UTC ISO format for consistency.
    """
    state = {}
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                state = json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning(f"⚠️ Could not read existing state file. A new one will be created.")

    # Standardize all incoming timestamps to UTC before saving.
    if timestamp.tzinfo is None:
        timestamp = timestamp.astimezone(timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)

    state.setdefault(source, {})[str(branch_id)] = timestamp.isoformat()

    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(state, f, indent=6)
        logger.debug(f"💾 Saved state for {source}/{branch_id} at {timestamp.isoformat()}")
    except IOError as e:
        logger.error(f"❌ Failed to save state to {CACHE_FILE}: {e}")




if __name__ == "__main__":
    # Setup basic logging to see the output from the functions
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
    )
    
    print("\n" + "="*50)
    print("🚀 Testing state_manager.py...")
    print("="*50)

    # 1. Define test parameters
    test_source = "test_bookings"
    test_branch_id = 999
    # Use a timezone-aware datetime for the test
    test_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
    
    try:
        # 2. Test the save function
        print(f"STEP 1: Saving timestamp for {test_source}/{test_branch_id}...")
        save_last_run_timestamp(test_source, test_branch_id, test_timestamp)
        print("✅ Save function executed.")
        
        # 3. Test the load function
        print(f"\nSTEP 2: Loading timestamp for {test_source}/{test_branch_id}...")
        loaded_timestamp = load_last_run_timestamp(test_source, test_branch_id)
        print(f"✅ Load function executed. Got: {loaded_timestamp}")
        
        # 4. Assert correctness
        print("\nSTEP 3: Verifying the timestamps match...")
        assert loaded_timestamp is not None, "Loaded timestamp should not be None!"
        # Compare timestamps up to the microsecond, as precision can vary slightly
        assert abs(loaded_timestamp - test_timestamp).total_seconds() < 0.001, "Loaded timestamp does not match saved timestamp!"
        print("✅ Verification successful!")

    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
    finally:
        # 5. Clean up the created test file
        print("\nSTEP 4: Cleaning up state file...")
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print(f"✅ Removed {CACHE_FILE}.")
    
    print("\n" + "="*50)
    print("🎉 state_manager.py test complete.")
    print("="*50)
