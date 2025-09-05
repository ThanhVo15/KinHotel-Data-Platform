# src/data_pipeline/extractors/pms/pms_extractor.py
import asyncio
import random
from datetime import datetime, timezone, timedelta
import logging
from typing import Dict, Any, List, Optional, Callable
from abc import abstractmethod

from ..abstract_extractor import AbstractExtractor, ExtractionResult
from ....utils.env_utils import get_config
from ....utils.state_manager import load_last_run_timestamp, save_last_run_timestamp
from ....utils.date_params import DateWindow, ICT

from ...clients.pms_client import PMSClient 

logger = logging.getLogger(__name__)

class PMSExtractor(AbstractExtractor):
    """Base PMS extractor: quản lý client & khung extract chung."""

    PARSERS: Dict[str, Callable[[Any], Any]] = {}

    TOKEN_BRANCH_MAP = {
        1:  "KIN HOTEL DONG DU",
        2:  "KIN HOTEL THI SACH EDITION",
        3:  "KIN HOTEL THAI VAN LUNG",
        4:  "KIN WANDER TAN BINH, THE MOUNTAIN",
        5:  "KIN WANDER TAN QUY",
        6:  "KIN WANDER TAN PHONG, THE MOONAGE",
        7:  "KIN WANDER TRUNG SON",
        9:  "KIN HOTEL CENTRAL PARK",
        10: "KIN HOTEL LY TU TRONG",
    }

    def __init__(self):
        super().__init__("PMS")
        cfg = get_config().get('pms', {})
        base_url = (cfg.get('base_url') or '').rstrip('/') + '/'
        self.client = PMSClient(base_url)

    # --------- Facade sang client (cho backward-compat) ---------
    async def _get_session(self, branch_id: int):
        return await self.client.get_session(branch_id)

    async def _make_request(self, session, url: str, params: Dict[str, Any]):
        return await self.client.get_json(session, url, params)

    async def _paginate(self, session, url: str, base_params: Dict[str, Any], *, limit_default: int = 100):
        # url đưa vào đây là dạng đầy đủ; để tái dùng client, ta trích endpoint
        # nhưng để đơn giản & không đụng nhiều, cho phép chuyền endpoint trực tiếp từ lớp con:
        # nếu url đã là base_url + endpoint, ta chỉ cần gọi client.paginate_json với endpoint
        # => tách endpoint từ url:
        endpoint = url.split(self.client.base_url)[-1]
        return await self.client.paginate_json(session, endpoint, base_params, limit_default=limit_default)

    async def close(self):
        await self.client.close()

    # ---------------- Template extract ----------------
    @abstractmethod
    async def _perform_extraction(self, session, branch_id: int, **kwargs) -> List[Any]:
        pass

    async def extract_async(self, *, branch_id: int, **kwargs) -> ExtractionResult:
        start_time = datetime.now(timezone.utc)
        branch_name = self.TOKEN_BRANCH_MAP.get(branch_id, f"Branch {branch_id}")
        source_name = f"PMS:{getattr(self, 'ENDPOINT', 'Unknown')}"

        try:
            session = await self._get_session(branch_id)
            self.logger.info(f"🚀 Starting extraction for {branch_name} - {source_name}")

            all_records = await self._perform_extraction(session, branch_id, **kwargs)

            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"✅ Extracted {len(all_records)} records from {branch_name} for endpoint '{source_name}' in {duration:.2f}s."
            )

            return ExtractionResult(
                data=all_records,
                source=source_name,
                branch_id=branch_id,
                branch_name=branch_name,
                record_count=len(all_records),
                **kwargs,
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.error(f"❌ Extraction failed for {branch_name} after {duration:.2f}s: {e}")
            return ExtractionResult(
                data=None,
                source=source_name,
                branch_id=branch_id,
                branch_name=branch_name,
                status="error",
                error=str(e),
                **kwargs,
            )

    def validate_config(self) -> bool:
        return bool(self.client and self.client.base_url)
    
        # ===== NEW: dựng DateWindow từ watermark state có sẵn =====
    def _build_window_from_state(
        self,
        *,
        endpoint: str,      # ví dụ: "bookings"
        field: str,         # "check_in" hoặc "create"
        branch_id: int,
        lookback_days: int = 1
    ) -> DateWindow:
        """
        Lấy watermark từ state (UTC). Nếu chưa có → dùng now - lookback_days.
        Cửa sổ: (last_run + 1s) → now  để tránh trùng mép.
        """
        source_key = f"{endpoint}:{field}"
        now_utc = datetime.now(timezone.utc)

        last_run_utc = load_last_run_timestamp(source_key, branch_id)
        if last_run_utc:
            start_utc = last_run_utc + timedelta(seconds=1)
        else:
            start_utc = now_utc - timedelta(days=lookback_days)

        return DateWindow.from_utc(start=start_utc, end=now_utc, field=field, tz=ICT)

    # ===== NEW: incremental cho nhiều branch, dùng watermark chuẩn =====
    async def extract_incremental(
        self,
        *,
        endpoint: str,              # ví dụ "bookings"
        field: str = "check_in",    # hoặc "create"
        branch_ids: Optional[List[int]] = None,
        lookback_days: int = 1,
        max_concurrent: int = 5,
        jitter_max_s: float = 0.8,
        **kwargs
    ) -> Dict[int, ExtractionResult]:
        """
        - Tạo DateWindow từ state (per endpoint+field+branch).
        - Gọi extract_async(branch_id, date_window=dw, **kwargs) cho từng branch.
        - Nếu OK → lưu watermark = dw.end (UTC) bằng state_manager hiện tại.
        """
        if branch_ids is None:
            branch_ids = list(self.TOKEN_BRANCH_MAP.keys())

        sem = asyncio.Semaphore(max_concurrent)
        results: Dict[int, ExtractionResult] = {}

        async def _run_one(bid: int):
            async with sem:
                try:
                    dw = self._build_window_from_state(
                        endpoint=endpoint, field=field, branch_id=bid, lookback_days=lookback_days
                    )
                    # Tránh cùng nổ vào page 1 → giảm dính WAF
                    await asyncio.sleep(random.random() * jitter_max_s)

                    res = await self.extract_async(branch_id=bid, date_window=dw, **kwargs)
                    results[bid] = res

                    if res.is_success:
                        # LƯU Ý: state_manager hiện tại nhận đối số theo đúng thứ tự (không keyword)
                        save_last_run_timestamp(f"{endpoint}:{field}", bid, dw.end.astimezone(timezone.utc))
                except Exception as e:
                    results[bid] = ExtractionResult(
                        data=None,
                        source=f"PMS:{endpoint}",
                        branch_id=bid,
                        branch_name=self.TOKEN_BRANCH_MAP.get(bid, f"Branch {bid}"),
                        status="error",
                        error=str(e),
                    )

        await asyncio.gather(*(_run_one(b) for b in branch_ids))
        return results

    # ===== NEW: triển khai abstract extract_multi_branch để hết TypeError =====
    async def extract_multi_branch(
        self,
        *,
        endpoint: str,
        branch_ids: Optional[List[int]] = None,
        field: str = "check_in",
        lookback_days: int = 1,
        max_concurrent: int = 5,
        **kwargs
    ) -> Dict[int, ExtractionResult]:
        """
        Wrapper mỏng gọi extract_incremental() để thỏa abstract method của AbstractExtractor.
        """
        return await self.extract_incremental(
            endpoint=endpoint,
            field=field,
            branch_ids=branch_ids,
            lookback_days=lookback_days,
            max_concurrent=max_concurrent,
            **kwargs
        )

