# src/dqx/utils/documentation.py
from __future__ import annotations

import json
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from utils.display import show_df, display_section

__all__ = [
    "_materialize_table_doc",
    "_q_fqn",
    "_esc_comment",
    "preview_table_documentation",
    "apply_table_documentation",
]


def _materialize_table_doc(doc_template: Dict[str, Any], table_fqn: str) -> Dict[str, Any]:
    """Fill the table_fqn into the doc template (deep copied), and expand {TABLE_FQN} placeholders."""
    copy = json.loads(json.dumps(doc_template))
    copy["table"] = table_fqn
    if "table_comment" in copy and isinstance(copy["table_comment"], str):
        copy["table_comment"] = copy["table_comment"].replace("{TABLE_FQN}", table_fqn)
    return copy


def _q_fqn(fqn: str) -> str:
    """Backtick-quote catalog.schema.table."""
    return ".".join(f"`{p}`" for p in fqn.split("."))


# =========================
# Comments + documentation
# =========================
def _esc_comment(s: str) -> str:
    """Escape single quotes for SQL COMMENT strings."""
    return (s or "").replace("'", "''")


def preview_table_documentation(spark: SparkSession, table_fqn: str, doc: Dict[str, Any]) -> None:
    display_section("TABLE METADATA PREVIEW (markdown text stored in comments)")
    doc_df = spark.createDataFrame(
        [(table_fqn, doc.get("table_comment", ""))],
        schema="table string, table_comment_markdown string",
    )
    show_df(doc_df, n=1, truncate=False)

    cols = doc.get("columns", {}) or {}
    cols_df = spark.createDataFrame(
        [(k, v) for k, v in cols.items()],
        schema="column string, column_comment_markdown string",
    )
    show_df(cols_df, n=200, truncate=False)


def apply_table_documentation(
    spark: SparkSession,
    table_fqn: str,
    doc: Optional[Dict[str, Any]],
) -> None:
    """Apply table comment and per-column comments, with fallbacks for engines that lack COMMENT support."""
    if not doc:
        return

    qtable = _q_fqn(table_fqn)

    # Table comment
    table_comment = _esc_comment(doc.get("table_comment", ""))
    if table_comment:
        try:
            spark.sql(f"COMMENT ON TABLE {qtable} IS '{table_comment}'")
        except Exception:
            # Fallback to TBLPROPERTIES for engines that don’t support COMMENT
            spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    # Column comments (always best-effort)
    cols: Dict[str, str] = doc.get("columns", {}) or {}
    existing_cols = {f.name.lower() for f in spark.table(table_fqn).schema.fields}
    for col_name, comment in cols.items():
        if col_name.lower() not in existing_cols:
            continue
        qcol = f"`{col_name}`"
        comment_sql = f"ALTER TABLE {qtable} ALTER COLUMN {qcol} COMMENT '{_esc_comment(comment)}'"
        try:
            spark.sql(comment_sql)
        except Exception:
            # Try COMMENT ON COLUMN as fallback
            try:
                spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{_esc_comment(comment)}'")
            except Exception as e2:
                print(f"[meta] Skipped column comment for {table_fqn}.{col_name}: {e2}")