# src/data_pipeline/extractors/pms/booking_list.py
import asyncio
import aiohttp
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
import random
import time

from ....utils.state_manager import load_last_run_timestamp, save_last_run_timestamp
from ....utils.date_params import DateWindow, DateField, ICT
from .pms_extractor import PMSExtractor, ExtractionResult


class BookingListExtractor(PMSExtractor):
    """
    Extractor for the 'bookings' endpoint with robust pagination, state management,
    and a central DateWindow → API params mapper.

    NOTE ON STATE KEYS:
      We persist last-run timestamps per (endpoint, field) pair:
        source = f"{ENDPOINT}:{field}"  # e.g., "bookings:check_in" or "bookings:create"
      This avoids mixing windows when you later switch from check_in_* → created_*.
    """

    ENDPOINT = "bookings"
    logger = logging.getLogger(__name__)

    async def _perform_extraction(
        self, 
        session: aiohttp.ClientSession, 
        branch_id: int, 
        params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Implement abstract method required by PMSExtractor.
        This delegates to our existing _paginate method.
        """
        url = f"{self.base_url}{self.ENDPOINT}"
        return await self._paginate(session, url, params)

    # ------------- Parsing -------------

    def _parse_response(self, data: Any) -> List[Dict[str, Any]]:
        """Flatten common nested pieces from the bookings payload into top-level keys."""
        if not data or "data" not in data:
            self.logger.warning(f"No 'data' array in API response for endpoint: {self.ENDPOINT}")
            return []

        raw_records = data.get("data", [])
        flattened: List[Dict[str, Any]] = []
        for rec in raw_records:
            try:
                attributes = rec.get("attributes", {}) or {}
                pricelist = rec.get("pricelist", {}) or {}
                room_status = rec.get("room_status", {}) or {}
                surveys = rec.get("surveys", {}) or {}

                flattened_rec = {
                    **rec,
                    "room_no": attributes.get("room_no", ""),
                    "pricelist_id": pricelist.get("id", ""),
                    "pricelist_name": pricelist.get("name", ""),
                    "room_is_clean": room_status.get("is_clean", False),
                    "room_is_occupied": room_status.get("is_occupied", False),
                    "survey_is_checkin": surveys.get("is_checkin", False),
                    "survey_is_checkout": surveys.get("is_checkout", False),
                }
                flattened.append(flattened_rec)
            except Exception as e:
                self.logger.error(f"Error parsing record: {e}")
        return flattened

    # ------------- Core pagination (single branch) -------------

    async def _paginate(self, session, url, base_params):
        all_records: List[Dict[str, Any]] = []
        page = 1
        limit = int(base_params.get("limit", 50))   # NEW: default 50 (was 100)

        while True:
            params = {**base_params, "page": page, "limit": limit}
            self.logger.info(f"🔍 Fetching page {page} …")
            data, status_code = await self._make_request(session, url, params)
            self.logger.info(f"🔍 Sleep Fetching page {page} in 5s…")
            time.sleep(5)

            if status_code >= 400:
                self.logger.warning(f"⚠️ Received status {status_code}. Stopping pagination.")
                break
            records = self._parse_response(data)
            got = len(records)
            if got == 0:
                self.logger.info("✅ No more records. Pagination complete.")
                break

            all_records.extend(records)

            # next-page decision via links/meta or batch-size heuristic (unchanged from earlier fix) …
            meta = data.get("meta") if isinstance(data, dict) else None
            links = data.get("links") if isinstance(data, dict) else None
            has_next = False
            if links and isinstance(links, dict):
                has_next = bool(links.get("next"))
            elif meta and isinstance(meta, dict):
                cur = meta.get("current_page"); last = meta.get("last_page")
                if isinstance(cur, int) and isinstance(last, int):
                    has_next = cur < last
            if not has_next and got < limit:
                self.logger.info(f"✅ Final page detected (got {got} < limit {limit}).")
                break

            page += 1
            await asyncio.sleep(0.6 + random.random() * 0.9)  # slightly larger jitter
        return all_records  

    # ------------- Public extraction APIs -------------

    async def extract_async(self, *, branch_id: int = 1, **kwargs) -> ExtractionResult:
        """
        Orchestrates pagination for a single branch with a DateWindow param.
        - Preferred usage: pass `date_window: DateWindow` in kwargs
        - Backward compat: if date_window is missing, we try legacy kwargs:
              {check_in_from, check_in_to} or {created_date_from, created_date_to}
          and synthesize a DateWindow on the fly.
        """
        start_ts = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
        url = f"{self.base_url}{self.ENDPOINT}"

        # ---- Resolve DateWindow (preferred) or fallback to legacy kwargs ----
        dw: Optional[DateWindow] = kwargs.pop("date_window", None)

        if dw is None:
            # Try to infer field from legacy kwargs
            check_in_from = kwargs.pop("check_in_from", None)
            check_in_to = kwargs.pop("check_in_to", None)
            created_from = kwargs.pop("created_date_from", None)
            created_to = kwargs.pop("created_date_to", None)

            def parse_dt(v: Any) -> Optional[datetime]:
                if v is None:
                    return None
                if isinstance(v, datetime):
                    return v
                # best-effort parse ISO or "YYYY-MM-DD HH:MM:SS"
                try:
                    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                except Exception:
                    self.logger.warning(f"Unrecognized datetime format: {v}")
                    return None

            if check_in_from or check_in_to:
                s = parse_dt(check_in_from) or (start_ts - timedelta(days=1))
                e = parse_dt(check_in_to) or start_ts
                dw = DateWindow(start=s, end=e, field="check_in", tz=ICT)
            elif created_from or created_to:
                s = parse_dt(created_from) or (start_ts - timedelta(days=1))
                e = parse_dt(created_to) or start_ts
                dw = DateWindow(start=s, end=e, field="create", tz=ICT)
            else:
                raise ValueError(
                    "Missing date_window. Provide DateWindow or legacy {check_in_*} / {created_date_*} params."
                )

        source_key = f"{self.ENDPOINT}:{dw.field}"

        try:
            session = await self._get_session(branch_id)

            base_params: Dict[str, Any] = {**dw.as_api_params(), **kwargs}
            self.logger.info(
                f"🚀 Starting extraction {source_key} for {branch_name}: "
                f"{base_params.get('check_in_from') or base_params.get('created_date_from')} → "
                f"{base_params.get('check_in_to') or base_params.get('created_date_to')}"
            )

            all_records = await self._paginate(session, url, base_params)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self.logger.info(
                f"✅ PMS {branch_name}: Extracted {len(all_records)} records in {duration:.2f}s"
            )

            # Persist last-run using UTC end of window
            save_last_run_timestamp(
                source=source_key,
                branch_id=branch_id,
                timestamp=dw.end.astimezone(timezone.utc),
            )

            return ExtractionResult(
                data=all_records,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_ts,
                record_count=len(all_records),
                status="success",
                created_date_from=dw.start.astimezone(timezone.utc),
                created_date_to=dw.end.astimezone(timezone.utc),
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self.logger.error(f"❌ PMS {branch_name} failed after {duration:.2f}s: {e}")
            return ExtractionResult(
                data=None,
                source=f"PMS:{self.ENDPOINT}",
                branch_id=branch_id,
                branch_name=branch_name,
                extracted_at=start_ts,
                status="error",
                error=str(e),
                created_date_from=dw.start.astimezone(timezone.utc) if dw else None,
                created_date_to=dw.end.astimezone(timezone.utc) if dw else None,
            )

    async def extract_bookings_incrementally(
        self,
        branch_ids: Optional[List[int]] = None,
        lookback_days: int = 1,
        *,
        field: DateField = "check_in",
        max_concurrent: int = 3,              # <-- thêm tham số chính danh
        **kwargs,
    ) -> Dict[int, ExtractionResult]:
        """
        Multi-branch incremental extraction with concurrency cap.
        State key: f"{ENDPOINT}:{field}" to keep watermarks separate per field.
        """
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        source_key = f"{self.ENDPOINT}:{field}"
        now_utc = datetime.now(timezone.utc)

        sem = asyncio.Semaphore(max_concurrent)

        async def _one_branch(bid: int) -> ExtractionResult:
            async with sem:
                try:
                    last_run_utc = load_last_run_timestamp(source=source_key, branch_id=bid)
                    start_utc = last_run_utc or (now_utc - timedelta(days=lookback_days))
                    dw = DateWindow.from_utc(start_utc, now_utc, field=field, tz=ICT)

                    self.logger.info(f"🗓️ Window for branch {bid} ({field}): {dw.start} → {dw.end} (UTC)")

                    # Lệch pha nhẹ để tránh cùng đập vào page 1
                    await asyncio.sleep(random.random() * 0.8)

                    # truyền limit/params khác qua kwargs → extract_async → _paginate
                    return await self.extract_async(branch_id=bid, date_window=dw, **kwargs)

                except Exception as e:
                    # Không để task vỡ bung: trả về ExtractionResult lỗi
                    return ExtractionResult(
                        data=None,
                        source=f"PMS:{self.ENDPOINT}",
                        branch_id=bid,
                        branch_name=self.TOKEN_BRANCH_MAP.get(bid, f"Branch {bid}"),
                        status="error",
                        error=str(e),
                        extracted_at=now_utc,
                        created_date_from=start_utc if 'start_utc' in locals() else None,
                        created_date_to=now_utc,
                    )

        results_list = await asyncio.gather(*[_one_branch(b) for b in branch_ids])
        return {res.branch_id: res for res in results_list}







