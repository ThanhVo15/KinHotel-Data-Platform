import asyncio
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone

from ....utils.state_manager import load_last_run_timestamp, save_last_run_timestamp
from .pms_extractor import PMSExtractor, ExtractionResult

class BookingListExtractor(PMSExtractor):
    """
    Specific Extractor for the 'bookings' endpoint with robust pagination and state management.
    """

    ENDPOINT = "bookings"

    def _parse_response(self, 
                        data: Any) -> List[Dict[str, Any]]:
        """Override to handle parsing and flattening nested fields from JSON"""
        if not data or "data" not in data:
            self.logger.warning(f"No Data records found in API response for endpoint: {self.ENDPOINT}")
            return []
        
        raw_records = data.get("data", [])
        flattened = []
        for rec in raw_records:
            try:
                attributes = rec.get("attributes", {}) or {}
                pricelist = rec.get("pricelist", {}) or {}
                room_status = rec.get("room_status", {}) or {}
                surveys = rec.get("surveys", {}) or {}

                flattened_rec ={
                    **rec,
                    'room_no': attributes.get('room_no', ''),
                    'pricelist_id': pricelist.get('id', ''), 
                    'pricelist_name': pricelist.get('name', ''),
                    'room_is_clean': room_status.get('is_clean', False),
                    'room_is_occupied': room_status.get('is_occupied', False),
                    'survey_is_checkin': surveys.get('is_checkin', False), 
                    'survey_is_checkout': surveys.get('is_checkout', False),
                }
                flattened.append(flattened_rec)
            except Exception as e:
                self.logger.error(f"Error parsing record: {str(e)}")
        return flattened
    
    async def extract_async(self, 
                        branch_id: int =1, 
                        **kwargs) ->ExtractionResult:
        """Override base to handle the pagination loop for bookings"""
        start_time = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
        self.logger.info(f"🚀 Starting paginated extraction for {branch_name}...")

        all_records = []
        page = 1
        url = f"{self.base_url}{self.ENDPOINT}"

        try:
            session = await self._get_session(branch_id)
            while True:
                params = { **kwargs,
                        'page': page, 
                        'limit': kwargs.get("limit", 100)}
                self.logger.info(f"🔍 Fetching page {page} for {branch_name} - {self.ENDPOINT}")

                data, status_code = await self._make_request(session, url, params)

                if status_code >= 400:
                    self.logger.warning(f"⚠️ Received status {status_code} on page {page}. Stopping pagination.")
                    break
                records = self._parse_response(data)

                if not records:
                    self.logger.info(f"✅ No more records found on page {page}. Pagination complete.")
                    break

                all_records.extend(records)
                page +=1
                await asyncio.sleep(0.1) # Be kind to the API to avoid rate-limiting.
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(f"✅ PMS {branch_name}: Extracted {len(all_records)} records (all pages) in {duration:.2f}s")

            run_end_time = kwargs.get("created_date_to", datetime.now(timezone.utc))

            save_last_run_timestamp(
                source= self.ENDPOINT,
                branch_id= branch_id,
                timestamp= run_end_time.astimezone(timezone.utc)
            )

            return ExtractionResult(
                data=all_records,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_time,
                record_count=len(all_records),
                status="success",
                created_date_from=kwargs.get('created_date_from'),
                created_date_to=run_end_time
            )
        
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.error(f"❌ PMS {branch_name} failed after {duration:.2f}s: {e}")
            return ExtractionResult(
                data=None,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_time,
                status="error",
                error=str(e),
                created_date_from=kwargs.get('created_date_from'),
                created_date_to=kwargs.get('created_date_to')
            )
        
    async def extract_bookings_incrementally(self,
                                            branch_ids: Optional[List[int]] = None,
                                            lookback_days: int = 1,
                                            **kwargs) -> Dict[int, ExtractionResult]:
        """User-facing method that automatically determines the date range for each branch."""
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())
        
        ict_timezone = timezone(timedelta(hours = 7))
        tasks = []

        for branch_id in branch_ids:
            # Step 1: Load the last run time (stored in UTC).
            last_run_utc = load_last_run_timestamp(source = self.ENDPOINT,
                                                   branch_id= branch_id)
            created_date_from_utc = (
                last_run_utc or 
                (datetime.now(timezone.utc) - timedelta(days = lookback_days))
            )
            created_date_to_utc = datetime.now(timezone.utc)

            # Step 2: Convert UTC times to local time (ICT) for the API.
            created_date_from_ict = created_date_from_utc.astimezone(ict_timezone)
            created_date_to_ict = created_date_to_utc.astimezone(ict_timezone)
            
            self.logger.info(
                f"🗓️ Extraction window for branch {branch_id} (ICT): "
                f"{created_date_from_ict.isoformat()} -> {created_date_to_ict.isoformat()}"
            )

            # Step 3: Create an extraction task with the correct local time parameters.
            task = self.extract_async(
                branch_id=branch_id,
                created_date_from = created_date_from_ict.isoformat(),
                created_date_to = created_date_to_ict.isoformat(),
                **kwargs
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        return {res.branch_id: res for res in results}





# --- ADDED: Unit Test using Mocking ---
# --- ADDED: Unit Test using Mocking ---
if __name__ == '__main__':
    import asyncio
    from unittest.mock import patch, AsyncMock
    
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')

    async def main_test():
        print("\n" + "="*60)
        print("🚀 Testing booking_list.py...")
        print("="*60)
        
        # Sửa đường dẫn patch cho đúng với cấu trúc thư mục
        with patch('src.utils.env_utils.get_config', return_value={'pms': {'base_url': 'https://fake-api.com/api/'}}), \
             patch('src.utils.token_manager.get_pms_token', return_value='fake-token'), \
             patch('src.utils.state_manager.save_last_run_timestamp') as mock_save, \
             patch('src.utils.state_manager.load_last_run_timestamp', return_value=None):

            # Import the class *inside* the patch context
            from src.data_pipeline.extractors.pms.booking_list import BookingListExtractor
            
            extractor = BookingListExtractor()
            
            page1_response = ({"data": [{"id": 101}, {"id": 102}]}, 200)
            page2_response = ({"data": [{"id": 201}]}, 200)
            empty_response = ({"data": []}, 200)
            
            extractor._make_request = AsyncMock(side_effect=[page1_response, page2_response, empty_response])
            
            print("--- Testing Pagination and Incremental Logic ---")
            results_dict = await extractor.extract_bookings_incrementally(branch_ids=[1])
            
            final_result = results_dict[1]
            print(f"\n✅ Final record count from test: {final_result.record_count}")
            
            assert final_result.record_count == 3
            print("✅ Correct total number of records (3) was aggregated.")
            
            mock_save.assert_called_once()
            print("✅ `save_last_run_timestamp` was called correctly.")

            await extractor.close()
        print("\n" + "="*60)
        print("🎉 booking_list.py test complete.")
        print("="*60)
            
    asyncio.run(main_test())