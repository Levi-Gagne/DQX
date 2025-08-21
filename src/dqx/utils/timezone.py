# src/dqx/utils/timezone.py

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


def current_time_iso(time_zone: Optional[str] = None) -> str:
    """
    Return the current datetime as an ISO 8601 string.
    - If a time zone name is provided AND available via zoneinfo, convert to that zone.
    - Otherwise, return UTC (never raises).
    """
    now_utc = datetime.now(timezone.utc)
    if time_zone and ZoneInfo is not None:
        try:
            return now_utc.astimezone(ZoneInfo(time_zone)).isoformat()
        except Exception:
            pass  # silently fall back to UTC
    return now_utc.isoformat()


__all__ = ["current_time_iso"]