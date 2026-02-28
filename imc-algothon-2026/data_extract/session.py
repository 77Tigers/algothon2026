from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

LONDON_TZ = ZoneInfo("Europe/London")


def settlement_session_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    """Return [start, end) for settlement window ending Sunday 12:00 London time."""
    if now is None:
        now = datetime.now(LONDON_TZ)
    else:
        now = now.astimezone(LONDON_TZ)
    days_until_sunday = (6 - now.weekday()) % 7
    this_sunday_noon = datetime.combine(
        now.date() + timedelta(days=days_until_sunday), time(12, 0), tzinfo=LONDON_TZ
    )
    end = this_sunday_noon + timedelta(days=7) if now >= this_sunday_noon else this_sunday_noon
    start = end - timedelta(days=1)
    return start, end
