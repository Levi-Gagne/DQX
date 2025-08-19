# src/dqx/utils/sqlfmt.py

from __future__ import annotations

def q_fqn(fqn: str) -> str:
    """Backtick-quote catalog.schema.table."""
    return ".".join(f"`{p}`" for p in fqn.split("."))

def esc_comment(s: str) -> str:
    """Escape single quotes for SQL COMMENT strings."""
    return (s or "").replace("'", "''")