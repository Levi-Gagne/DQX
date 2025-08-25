# src/dqx/utils/display.py

from __future__ import annotations

import inspect
from pyspark.sql import DataFrame

from framework_utils.color import Color

def _find_databricks_display():
    """Find the Databricks 'display' callable by walking caller frames."""
    try:
        f = inspect.currentframe()
        while f:
            g = f.f_globals
            d = g.get("display")
            if callable(d):
                return d
            f = f.f_back
    except Exception:
        pass
    return None

def show_df(df: DataFrame, n: int = 100, truncate: bool = False) -> None:
    """Render a DataFrame — prefer Databricks 'display' when available."""
    disp = _find_databricks_display()
    if disp:
        disp(df.limit(n))
    else:
        df.show(n, truncate=truncate)

def display_section(title: str) -> None:
    """Print a colored section header (plain print, no table)."""
    print("\n" + f"{Color.b}{Color.deep_magenta}═{Color.r}" * 80)
    print(f"{Color.b}{Color.deep_magenta}║{Color.r} {Color.b}{Color.ghost_white}{title}{Color.r}")
    print(f"{Color.b}{Color.deep_magenta}═{Color.r}" * 80)

__all__ = ["show_df", "display_section"]