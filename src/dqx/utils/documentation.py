# src/dqx/utils/documentation.py

from __future__ import annotations

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from utils.display import show_df, display_section

__all__ = [
    "_q_fqn",
    "_esc_comment",
    "build_doc_from_schema",
    "preview_table_documentation",
    "apply_table_documentation",
]

def _q_fqn(fqn: str) -> str:
    """Backtick-quote catalog.schema.table."""
    return ".".join(f"`{p}`" for p in fqn.split("."))

def _esc_comment(s: str) -> str:
    """Escape single quotes for SQL COMMENT strings."""
    return (s or "").replace("'", "''")

def build_doc_from_schema(
    table_description: Optional[str],
    schema: T.StructType,
    *,
    metadata_key: str = "comment",
) -> Dict[str, Any]:
    """
    Create a documentation payload from a StructType and an optional table_description.
    - Table comment comes from the YAML 'table_description' (string, may be markdown).
    - Column comments come from StructField.metadata[metadata_key], if present.
    """
    cols: Dict[str, str] = {}
    for field in schema.fields:
        try:
            meta = field.metadata or {}
            comment = meta.get(metadata_key)
            if isinstance(comment, str) and comment.strip():
                cols[field.name] = comment
        except Exception:
            # Be permissive; skip if metadata not readable.
            pass

    return {
        "table_comment": table_description or "",
        "columns": cols,
    }

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
    """
    Apply table comment and column comments via SQL COMMENT statements.
    Falls back to TBLPROPERTIES('comment'='...') for engines that don't support COMMENT ON TABLE.
    """
    if not doc:
        return

    qtable = _q_fqn(table_fqn)

    # Table comment
    table_comment = _esc_comment(doc.get("table_comment", ""))
    if table_comment:
        try:
            spark.sql(f"COMMENT ON TABLE {qtable} IS '{table_comment}'")
        except Exception:
            spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    # Column comments (best-effort)
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
            try:
                spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{_esc_comment(comment)}'")
            except Exception as e2:
                print(f"[meta] Skipped column comment for {table_fqn}.{col_name}: {e2}")