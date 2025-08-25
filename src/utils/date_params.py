# src/data_pipeline/utils/date_params.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Literal, Optional

DateField = Literal["check_in", "create"]  # extend later: "update", "actual_check_in", ...

# Default KIN PMS timezone (ICT = UTC+7). We keep a named constant for clarity.
ICT = timezone(timedelta(hours=7))


@dataclass(frozen=True)
class DateWindow:
    """
    Canonical representation of a date window for PMS API requests.
    - `start` / `end` are timezone-aware (preferred) or naive (assumed UTC).
    - `field` chooses which pair of API params to emit (check_in_* or created_*).
    - `tz` is the target timezone that the API expects in the query (default ICT).
    """
    start: datetime
    end: datetime
    field: DateField = "check_in"
    tz: timezone = ICT

    def _ensure_tz(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware; treat naive as UTC."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _fmt_api(self, dt: datetime) -> str:
        """
        Format to 'YYYY-MM-DD HH:MM:SS' in the API's local timezone (ICT).
        Let the HTTP client urlencode spaces to '+' automatically.
        """
        dt = self._ensure_tz(dt).astimezone(self.tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def as_api_params(self) -> Dict[str, str]:
        """Map the window → API params without the caller caring which field is active."""
        start_s = self._fmt_api(self.start)
        end_s = self._fmt_api(self.end)

        if self.field == "check_in":
            return {"check_in_from": start_s, "check_in_to": end_s}
        if self.field == "create":
            return {"created_date_from": start_s, "created_date_to": end_s}
        raise ValueError(f"Unsupported field: {self.field}")

    # --------- Conveniences (optional) ---------

    @staticmethod
    def from_utc(
        start_utc: datetime,
        end_utc: Optional[datetime] = None,
        *,
        field: DateField = "check_in",
        tz: timezone = ICT,
    ) -> "DateWindow":
        """Build directly from UTC datetimes (end defaults to now UTC)."""
        if end_utc is None:
            end_utc = datetime.now(timezone.utc)
        return DateWindow(start=start_utc, end=end_utc, field=field, tz=tz)

    @staticmethod
    def lookback_days(
        days: int,
        *,
        field: DateField = "check_in",
        tz: timezone = ICT,
    ) -> "DateWindow":
        """Convenience for 'last N days until now' in UTC, formatted to tz."""
        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(days=max(days, 0))
        return DateWindow(start=start_utc, end=end_utc, field=field, tz=tz)
