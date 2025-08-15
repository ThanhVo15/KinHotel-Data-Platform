import os
import sys
import logging
from typing import Any, Dict, List
from datetime import datetime

from ..abstract_extractor import AbstractExtractor, ExtractionResult
from .pms_extractor import PMSExtractor

class BookingListExtractor(PMSExtractor):
    """Specific Extractor for 'api/bookings' endpoint."""

    def _parse_response(self, 
                        data: Any, 
                        endpoint: str) -> List[Dict[str, Any]]:
        """Override to handle parsing and flattening nested fields from JSON"""
        if data is None:
            self.logger.warning(f"Received None response for {endpoint}")
            return []
        
        raw_records = data.get("data", [])
        if not raw_records:
            self.logger.warning(f"No data records found in APi response for {endpoint}")
            return []
        
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
                        endpoint: str, 
                        branch_id: int =1, 
                        start_date: str =None, 
                        end_date: str = None, 
                        created_date_from: datetime = None, 
                        created_date_to: datetime = None, 
                        limit: int = 100,
                        **kwargs) ->ExtractionResult:
        """Override base to handle the pagination loop for bookings"""
        try:
            start_time = datetime.now()
            branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
            self.logger.info(f"🔍 Extracting from {branch_name} - {endpoint}")

            base_params = {}
            if start_date:
                base_params['start_date'] = start_date
            if end_date:
                base_params['end_date'] = end_date
            if created_date_from and created_date_to:
                base_params['created_date_from'] = created_date_from.isoformat()
                base_params['created_date_to'] = created_date_to.isoformat()
            base_params.update(kwargs)

            self.logger.info(f"Parameters: {base_params}")

            session = await self._get_session(branch_id)
            url = f"{self.base_url}{endpoint}"

            all_records = []
            page = 1
            total = 0
            record_count = 0

            while True:
                params = {'page': page, 
                        'limit': limit,
                        **base_params}
                self.logger.info(f"🔍 Fetching page {page} for {branch_name} - {endpoint}")

                try:
                    async with session.get(url, params=params) as response:
                        # Special handling for 403 errors on pages after the first
                        if response.status == 403 and page > 1:
                            self.logger.warning(f"⚠️ No more pages available for {branch_name} (403 on page {page})")
                            break
                            
                        response.raise_for_status()
                        data = await response.json()

                        if not data:
                            self.logger.warning(f"Empty response for {branch_name}")
                            break

                        records = self._parse_response(data, endpoint)
                        all_records.extend(records)

                        pagination = data.get('pagination', {})
                        total = pagination.get('total', 0)
                        current_limit = pagination.get('limit', limit)
                        
                        # Check if we've reached the end of data
                        if len(records) < current_limit or page * current_limit >= total:
                            break
                        page += 1
                except Exception as e:
                    if hasattr(e, 'status') and e.status == 403 and page > 1:
                        # We've likely reached the end of available pages
                        self.logger.warning(f"⚠️ No more pages available for {branch_name} (403 on page {page})")
                        break
                    else:
                        # For other errors or first page failures, propagate the error
                        raise

            record_count = len(all_records)
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"✅ PMS {branch_name}: {record_count} records (all pages) in {duration:.2f}s")

            return ExtractionResult(
                data=all_records,
                source=f"PMS:{endpoint}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_time,
                record_count=record_count,
                status="success",
                created_date_from=created_date_from,
                created_date_to=created_date_to
            )
        
        except Exception as e:
            self.logger.error(f"❌ PMS {branch_name} failed: {e}")
            return ExtractionResult(
                data=None,
                source=f"PMS:{endpoint}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=datetime.now(),
                record_count=0,
                status="error",
                error=str(e),
                start_date=start_date,
                end_date=end_date
            )
        
    async def extract_bookings(self,
                               branch_ids: List[int] = None,
                               check_in_from: str = None,
                               check_in_to: str = None,
                               limit: int = 100) -> Dict[int, ExtractionResult]:
        """User-facing multi-branch method for bookings."""
        if check_in_from is None:
            check_in_from = datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
        elif ' ' not in check_in_from:
            check_in_from = check_in_from + ' 00:00:00'
            
        if check_in_to is None:
            check_in_to = datetime.now().strftime('%Y-%m-%d') + ' 23:59:59'
        elif ' ' not in check_in_to:
            check_in_to = check_in_to + ' 23:59:59'
        return await self.extract_multi_branch(
            endpoint="bookings",
            branch_ids=branch_ids,
            check_in_from=check_in_from,
            check_in_to=check_in_to,
            limit=limit 
        )
    
    async def close_all(self):
        """Close all open aiohttp sessions"""
        for branch_id, session in list(self.sessions.items()):
            if not session.closed:
                await session.close()
                logger.debug(f"🔒 Closed session for branch {branch_id}")
        self.sessions = {}
        logger.info(f"🔒 All sessions closed")

# ...existing code...

if __name__ == "__main__":
    import asyncio
    import sys
    import logging
    from datetime import datetime, timedelta

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger("booking_extractor")

    async def main():
        """Run booking extraction demo with simplified date input"""
        try:
            # Create extractor
            extractor = BookingListExtractor()
            logger.info("🚀 Starting BookingListExtractor...")
            
            # Simple date handling - either use arguments or defaults
            check_in_from = datetime.now().strftime('%Y-%m-%d')  # Default to today
            check_in_to = check_in_from
            
            # Get date parameters if provided
            if len(sys.argv) > 1:
                check_in_from = sys.argv[1]
            if len(sys.argv) > 2:
                check_in_to = sys.argv[2]
                
            # Print extraction parameters
            logger.info(f"📅 Extracting bookings from {check_in_from} to {check_in_to}")
            logger.info(f"🏢 Branches: All branches")
            
            # Run extraction
            start_time = datetime.now()
            results = await extractor.extract_bookings(
                check_in_from=check_in_from,
                check_in_to=check_in_to,
                limit=15
            )
            
            # Display simple results summary
            print("\n" + "="*60)
            print(f"📊 BOOKING EXTRACTION RESULTS")
            print("="*60)
            
            total_bookings = 0
            success_count = 0
            
            for branch_id, result in results.items():
                branch_name = extractor.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
                status_icon = "✅" if result.is_success else "❌"
                
                print(f"{status_icon} {branch_name}: ", end="")
                if result.is_success:
                    record_count = result.record_count
                    total_bookings += record_count
                    success_count += 1
                    print(f"{record_count} bookings")
                else:
                    print(f"Failed: {result.error}")
            
            duration = (datetime.now() - start_time).total_seconds()
            print("\n" + "-"*60)
            print(f"✅ Success: {success_count}/{len(results)} branches")
            print(f"📊 Total bookings: {total_bookings}")
            print(f"⏱️ Duration: {duration:.2f}s")
            print("="*60)
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            raise
        finally:
            # Always close sessions
            if 'extractor' in locals():
                logger.info("🧹 Cleaning up...")
                await extractor.close_all()
    
    # Run the async main function
    asyncio.run(main())