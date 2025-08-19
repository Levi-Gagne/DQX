So can we remove this: 
    safe_tz = time_zone
    try:
        _ = current_time_iso(time_zone)
    except Exception:
        safe_tz = "UTC"
    print_notebook_env(spark, local_timezone=safe_tz)



I want to call the timezone like this:     time_zone: str = "America/Chicago",



I really dont want to overcomplicate this and this is the timer file:

# src/dqx/utils/timer.py


from datetime import datetime, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

def current_time_iso(tz_name: Optional[str] = "UTC") -> str:
    # Prefer requested zone; fall back to UTC if tz data isn't available.
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name or "UTC")).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()
