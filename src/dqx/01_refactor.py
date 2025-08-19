I now get this errro: 
NameError: name 'apply_table_documentation' is not defined
File <command-4730582016129158>, line 545
    531     return main(
    532         output_config_path=dqx_cfg_yaml,
    533         rules_dir=None,
   (...)
    541         apply_table_metadata=apply_table_metadata,
    542     )
    544 # ---- run it ----
--> 545 res = load_checks(
    546     dqx_cfg_yaml="resources/dqx_config.yaml",
    547     # overrides are optional; omit to use YAML-configured values
    548 )
    549 print(res)
File /Workspace/Users/levi.gagne@claconnect.com/DQX/src/dqx/utils/table.py:187, in create_table_if_absent(spark, table_fqn, schema, table_doc, primary_key, partition_by)
    185 # Apply docs once at create
    186 if table_doc:
--> 187     apply_table_documentation(spark, table_fqn, table_doc)
    189 # Add PRIMARY KEY once at create
    190 if primary_key:



heres the table file: 
# src/utils/table.py

from __future__ import annotations

from typing import Tuple, List, Optional
from pyspark.sql import SparkSession, DataFrame, types as T
from typing import Any
from utils.console import Console  # <-- switched from utils.printer.Print

__all__ = [
    "table_exists", "refresh_table", "spark_sql_to_df", "spark_df_to_rows",
    "is_view",
    "is_fully_qualified_table_name",
    "parse_fully_qualified_table_name",
    "parse_catalog_schema_fqn",
    "qualify_table_name",
    "qualify_with_schema_fqn",
    "ensure_fully_qualified",
    "empty_df_from_schema",
    "write_empty_delta_table",
    "add_primary_key_constraint",
]

# -------- Spark helpers --------

def table_exists(spark: SparkSession, fully_qualified_table: str) -> bool:
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"{Console.ERROR}Exception in table_exists({fully_qualified_table}): {e}")
        return False

def refresh_table(spark: SparkSession, fq: str) -> None:
    try:
        spark.sql(f"REFRESH TABLE {fq}")
    except Exception as e:
        msg = str(e)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in msg.upper() or "SERVERLESS" in msg.upper():
            print(f"{Console.WARN}Skipping REFRESH TABLE on serverless for {fq}.")
            return
        raise

def spark_sql_to_df(spark: SparkSession, sql: str) -> DataFrame:
    try:
        return spark.sql(sql)
    except Exception as e:
        print(f"{Console.ERROR}spark_sql_to_df failed: {e}\nSQL: {sql}")
        raise

def spark_df_to_rows(df: DataFrame) -> List[dict]:
    try:
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"{Console.ERROR}spark_df_to_rows failed: {e}")
        raise

# -------- UC identity & FQN helpers --------

def is_view(table_type: Optional[str]) -> bool:
    t = (table_type or "").upper()
    return t in {"VIEW", "MATERIALIZED_VIEW"}

def is_fully_qualified_table_name(name: str) -> bool:
    return isinstance(name, str) and name.count(".") == 2

def parse_fully_qualified_table_name(fq_table: str) -> Tuple[str, str, str]:
    if not isinstance(fq_table, str):
        print(f"{Console.ERROR}fq_table must be a string, got {type(fq_table).__name__}")
        raise TypeError("fq_table must be a string.")
    parts = fq_table.split(".")
    if len(parts) != 3:
        print(f"{Console.ERROR}Expected catalog.schema.table, got: {fq_table!r}")
        raise ValueError("Expected catalog.schema.table format.")
    return parts[0], parts[1], parts[2]

def parse_catalog_schema_fqn(schema_fqn: str) -> Tuple[str, str]:
    if not isinstance(schema_fqn, str):
        print(f"{Console.ERROR}schema_fqn must be a string, got {type(schema_fqn).__name__}")
        raise TypeError("schema_fqn must be a string.")
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        print(f"{Console.ERROR}Expected 'catalog.schema', got: {schema_fqn!r}")
        raise ValueError(f"Expected 'catalog.schema', got: {schema_fqn!r}")
    return parts[0], parts[1]

def qualify_table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"

def qualify_with_schema_fqn(schema_fqn: str, name: str) -> str:
    if is_fully_qualified_table_name(name):
        return name
    cat, sch = parse_catalog_schema_fqn(schema_fqn)
    return qualify_table_name(cat, sch, name)

def ensure_fully_qualified(
    name: str,
    *,
    default_schema_fqn: Optional[str] = None,
    default_catalog: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> str:
    if is_fully_qualified_table_name(name):
        return name
    if default_schema_fqn:
        return qualify_with_schema_fqn(default_schema_fqn, name)
    if default_catalog and default_schema:
        return qualify_table_name(default_catalog, default_schema, name)
    raise ValueError(f"Not fully qualified and no defaults provided: {name!r}")

def _quote_table(fqn: str) -> str:
    return ".".join(f"`{p}`" for p in fqn.split("."))

# -------- DataFrame/table creation helpers --------

def empty_df_from_schema(spark: SparkSession, schema: T.StructType) -> DataFrame:
    return spark.createDataFrame([], schema)

def write_empty_delta_table(
    spark: SparkSession,
    table_fqn: str,
    schema: T.StructType,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Create an empty Delta table with the exact schema. Does not create catalog/schema.
    """
    df = empty_df_from_schema(spark, schema)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_fqn)

# -------- Constraints helpers --------

def add_primary_key_constraint(
    spark: SparkSession,
    table_fqn: str,
    column: str = "check_id",
    constraint_name: Optional[str] = None,  # ignored (kept for backward compatibility)
    rely: bool = True,                       # ignored (kept for backward compatibility)
) -> None:
    """
    Create/declare a PRIMARY KEY on the table using the exact SQL you specified:
        ALTER TABLE <fq> ADD PRIMARY KEY (<cols>)

    `column` may be a single column name (str) or a list/tuple for a composite key.
    """
    qtable = _quote_table(table_fqn)

    if isinstance(column, (list, tuple)):
        cols_sql = ", ".join(f"`{c}`" for c in column)
        cols_label = ", ".join(str(c) for c in column)
    else:
        cols_sql = f"`{column}`"
        cols_label = str(column)

    sql = f"ALTER TABLE {qtable} ADD PRIMARY KEY ({cols_sql})"
    try:
        spark.sql(sql)
        print(f"{Console.SUCCESS}Added PRIMARY KEY on {table_fqn}: ({cols_label})")
    except Exception as e:
        print(f"{Console.WARN}Failed to set PRIMARY KEY on {table_fqn}: {e}")


def create_table_if_absent(
    spark: SparkSession,
    table_fqn: str,
    *,
    schema: T.StructType,
    table_doc: Optional[Dict[str, Any]] = None,
    primary_key: Optional[str] = "check_id",
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Create once with the exact schema, optional partitioning, table/column comments, and PK.
    Does not attempt to create catalog/schema; if they don't exist this will raise (as intended).
    """
    if table_exists(spark, table_fqn):
        return

    # Create the empty Delta table with exact schema
    write_empty_delta_table(spark, table_fqn, schema, partition_by=partition_by)

    # Apply docs once at create
    if table_doc:
        apply_table_documentation(spark, table_fqn, table_doc)

    # Add PRIMARY KEY once at create
    if primary_key:
        add_primary_key_constraint(
            spark, table_fqn, column=primary_key, constraint_name=f"pk_{primary_key}", rely=True
        )




It should get teh values form the the config now, I also have this doc umentaiton file, show me what needs updated:
# src/dqx/utils/documentation.py

from __future__ import annotations

import json
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from utils.display import show_df, display_section
from utils.config import ProjectConfig

__all__ = [
    "_materialize_table_doc",
    "_q_fqn",
    "_esc_comment",
    "preview_table_documentation",
    "apply_table_documentation",
    "doc_from_config",
]


def _materialize_table_doc(doc_template: Dict[str, Any], table_fqn: str) -> Dict[str, Any]:
    """Fill the table FQN into the doc template and expand placeholders."""
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


# Convenience: fetch and materialize table doc directly from YAML config
def doc_from_config(cfg: ProjectConfig, table_section: str, table_fqn: str) -> Optional[Dict[str, Any]]:
    """
    table_section: 'checks_config_table' | 'checks_log_table' | ...
    """
    tpl = cfg.table_doc(table_section)
    if not tpl:
        return None
    return _materialize_table_doc(tpl, table_fqn)
