import asyncio
import logging
from typing import Any, Dict, List

from .pms_extractor import PMSExtractor, ExtractionResult

logger = logging.getLogger(__name__)

class EInvoiceStatusExtractor(PMSExtractor):
    """
    Extractor for the 'e-invoice-status' endpoint.
    This endpoint is not paginated; it returns all data for a specific 'check_out_date'.
    """

    ENDPOINT = "bookings/e-invoice-status"

    def _parse_response(self,
                        data: Any) -> List[Dict[str, Any]]:
        """
        Overrides the base parser to handle the specific structure of the e-invoice API.
        It flattens the nested 'sale_order_info' list.
        """

        if not data or "data" not in data:
            self.logger.warning((f"No data records found in API response for endpoint: {self.ENDPOINT}"))
            return []
        
        raw_bookings = data.get("data", [])
        flattened_records = []

        for booking in raw_bookings:
            sale_orders = booking.get("sale_order_info", [])

            if not sale_orders:
                base_record = {
                    'booking_id': booking.get('booking_id'),
                    'guest_name': booking.get('guest_name'),
                    'travel_agency_name': booking.get('travel_agency_name'),
                    'check_in_date': booking.get('check_in_date'),
                    'check_out_date': booking.get('check_out_date'),
                    'sale_order_name': None,
                    'minvoice_signed': None,+
                    'einvoice_option': None,
                }
                flattened_records.append(base_record)
            else:
                for sale_order in sale_orders:
                    record = {
                        'booking_id': booking.get('booking_id'),
                        'guest_name': booking.get('guest_name'),
                        'travel_agency_name': booking.get('travel_agency_name'),
                        'check_in_date': booking.get('check_in_date'),
                        'check_out_date': booking.get('check_out_date'),
                        'sale_order_name': sale_order.get('sale_order_name'),
                        'minvoice_signed': sale_order.get('minvoice_signed'),
                        'einvoice_option': sale_order.get('einvoice_option'),
                    }
                flattened_records.append(record)
        return flattened_records
    
    async def _perform_extraction(self,
                                  sessions: Any,
                                  branch_id: int,
                                  **kwargs) -> List[Dict[str, Any]]:
        """
        Implements the core extraction logic for a single branch.
        Since there's no pagination, this is a single API call.
        """

        check_out_date = kwargs.get("check_out_date")
        if not check_out_date:
            raise ValueError("'check_out_date' is a required parameter for this extractor.")
        
        url = f"{self.base_url}{self.ENDPOINT}"
        params = {"check_out_date": check_out_date}
        session = await self._get_session(branch_id)

        data, status_code = await self._make_request(session, url, params)
        
        if status_code >= 400:
            self.logger.warning(f"Request failed with status {status_code}. Returning empty list.")
            return []
        
        return self._parse_response(data)
    
    async def extract_for_date(self,
                               check_out_date: str,
                               branch_ids: List[int] = None) -> Dict[int, ExtractionResult]:
        """
        User-facing method to extract e-invoice status for a specific checkout date
        across multiple branches.
        """

        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        self.logger.info(
            f"🚀 Starting e-invoice extraction for checkout date: {check_out_date} "
            f"across {len(branch_ids)} branches."
        )

        tasks = []
        for b_id in branch_ids:
            task = self.extract_async(
                branch_id= b_id,
                check_out_date = check_out_date
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        return {res.branch_id: res for res in results}



# --- Example Usage & Unit Test ---
if __name__ == '__main__':
    from unittest.mock import patch, AsyncMock

    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')

    async def main_test():
        print("\n" + "="*60)
        print("🚀 Testing e_invoice_status.py...")
        print("="*60)

        # Mock the external dependencies
        with patch('src.utils.env_utils.get_config', return_value={'pms': {'base_url': 'https://fake-api.com/api/'}}), \
             patch('src.utils.token_manager.get_pms_token', return_value='fake-token'):
            
            from src.data_pipeline.extractors.pms.e_invoice_status import EInvoiceStatusExtractor
            
            extractor = EInvoiceStatusExtractor()

            # Mock the API response
            mock_api_response = (
                {
                    "data": [
                        {
                            "booking_id": 22598,
                            "sale_order_info": [
                                {"sale_order_name": "S41467", "minvoice_signed": True, "einvoice_option": "unrequested"}
                            ],
                            "guest_name": "NG PENG BOON",
                            "check_out_date": "2025-08-01"
                        },
                        {
                            "booking_id": 22599,
                            "sale_order_info": [], # Test case with no sale orders
                            "guest_name": "TAY SWEE MOW",
                            "check_out_date": "2025-08-01"
                        }
                    ]
                },
                200
            )
            extractor._make_request = AsyncMock(return_value=mock_api_response)

            print("--- Testing Single Branch Extraction for a Specific Date ---")
            results_dict = await extractor.extract_for_date(check_out_date="2025-08-01", branch_ids= None)
            
            final_result = results_dict[1]
            print(f"\n✅ Extraction status: {final_result.status}")
            print(f"✅ Final record count from test: {final_result.record_count}")
            print("✅ Flattened Data Example:", final_result.data)

            # Assert that the data was flattened correctly (2 bookings -> 2 records)
            assert final_result.is_success
            assert final_result.record_count == 2
            # Check if the second record has None for sale_order fields
            assert final_result.data[1]['sale_order_name'] is None
            
            print("\n✅ Flattening logic verified successfully.")

            await extractor.close()

        print("\n" + "="*60)
        print("🎉 e_invoice_status.py test complete.")
        print("="*60)
            
    asyncio.run(main_test())