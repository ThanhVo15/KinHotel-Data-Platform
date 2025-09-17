from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Literal

# ICT = UTC+7
ICT = timezone(timedelta(hours=7))

DateField = Literal["check_in", "create"]

@dataclass
class DateWindow:
    start: datetime   # timezone-aware
    end: datetime     # timezone-aware
    field: DateField
    tz: timezone = ICT

    def as_api_params(self) -> Dict[str, str]:
        """
        Chuẩn hoá key theo field:
        - check_in  → check_in_from / check_in_to
        - create    → created_date_from / created_date_to
        Format: 'YYYY-MM-DD HH:MM:SS' (theo tz)
        """
        s = self.start.astimezone(self.tz).strftime("%Y-%m-%d %H:%M:%S")
        e = self.end.astimezone(self.tz).strftime("%Y-%m-%d %H:%M:%S")
        if self.field == "check_in":
            return {"check_in_from": s, "check_in_to": e}
        elif self.field == "create":
            return {"created_date_from": s, "created_date_to": e}
        else:
            raise ValueError("field must be 'check_in' or 'create'")

    @staticmethod
    def from_utc(start_utc: datetime, end_utc: datetime, *, field: DateField, tz: timezone = ICT) -> "DateWindow":
        if start_utc.tzinfo is None or end_utc.tzinfo is None:
            raise ValueError("Provide timezone-aware datetimes (UTC).")
        return DateWindow(start=start_utc, end=end_utc, field=field, tz=tz)

    @staticmethod
    def lookback_days(days: int, *, field: DateField, tz: timezone = ICT) -> "DateWindow":
        now = datetime.now(timezone.utc)
        return DateWindow(start=now - timedelta(days=days), end=now, field=field, tz=tz)
