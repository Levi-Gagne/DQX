# utils/timer.py
from datetime import datetime, timezone
def current_time_iso(tz: str = "UTC") -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(tz)).isoformat()
    except Exception:
        # Fallback if tz database isn't available
        return datetime.now(timezone.utc).isoformat()