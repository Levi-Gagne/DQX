# src/dqx/utils/timezone.py

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

def current_time_iso(time_zone: Optional[str] = "UTC") -> str:
    """
    Return the current datetime as an ISO 8601 string with timezone.
    """
    try:
        zone = ZoneInfo(time_zone)
    except Exception as e:
        raise ValueError(
            f"Invalid timezone '{time_zone}'. Must be a valid IANA timezone string. "
            "Examples: 'UTC', 'America/Chicago', 'Europe/Berlin'."
        ) from e
    return datetime.now(zone).isoformat()