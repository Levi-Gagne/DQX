So this code here: 
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
    Create once with the exact Spark schema (NOT NULLs honored), optional partitioning,
    table + column comments, and PRIMARY KEY. No-op if table already exists.
    """
    if table_exists(spark, table_fqn):
        return

    ensure_schema_exists(spark, table_fqn)

    # Create empty table with exact schema
    empty_df = spark.createDataFrame([], schema)
    writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_fqn)

    # Apply docs once at create
    if table_doc:
        apply_table_documentation(spark, table_fqn, table_doc)

    # Add PRIMARY KEY once at create (Unity Catalog requires NOT NULL; schema already sets it)
    if primary_key:
        qtable = _q_fqn(table_fqn)
        try:
            spark.sql(
                f"ALTER TABLE {qtable} "
                f"ADD CONSTRAINT pk_{primary_key} PRIMARY KEY (`{primary_key}`) RELY"
            )
        except Exception as e:
            print(f"[meta] Skipped PRIMARY KEY add on create for {table_fqn}.{primary_key}: {e}")


So one thing, can we make this its own functin:
    if primary_key:
        qtable = _q_fqn(table_fqn)
        try:
            spark.sql(
                f"ALTER TABLE {qtable} "
                f"ADD CONSTRAINT pk_{primary_key} PRIMARY KEY (`{primary_key}`) RELY"
            )
        except Exception as e:
            print(f"[meta] Skipped PRIMARY KEY add on create for {table_fqn}.{primary_key}: {e}")

I think this could also be a functino:
    writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_fqn)

This could possibly be a function?: empty_df = spark.createDataFrame([], schema)


Hre i have no access to create schema so theres no point, we should just try to crate the table and if the schema doesnt exist it will error
def ensure_schema_exists(spark: SparkSession, full_table_name: str):
    # Use your existing helper to parse the FQN; create only the schema part.
    catalog, schema, _ = parse_fully_qualified_table_name(full_table_name)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


Then maybe a single runner to run all of those, are those functions closely related enought to add to this file here?:
# src/utils/table.py

from __future__ import annotations

from typing import Tuple, List, Optional
from pyspark.sql import SparkSession, DataFrame
from utils.printer import Print

__all__ = [
    "table_exists", "refresh_table", "spark_sql_to_df", "spark_df_to_rows",
    "is_view",
    "is_fully_qualified_table_name",
    "parse_fully_qualified_table_name",
    "parse_catalog_schema_fqn",
    "qualify_table_name",
    "qualify_with_schema_fqn",
    "ensure_fully_qualified",
]

# -------- Spark helpers --------

def table_exists(spark: SparkSession, fully_qualified_table: str) -> bool:
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"{Print.ERROR}Exception in table_exists({fully_qualified_table}): {e}")
        return False

def refresh_table(spark: SparkSession, fq: str) -> None:
    try:
        spark.sql(f"REFRESH TABLE {fq}")
    except Exception as e:
        msg = str(e)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in msg.upper() or "SERVERLESS" in msg.upper():
            print(f"{Print.WARN}Skipping REFRESH TABLE on serverless for {fq}.")
            return
        raise

def spark_sql_to_df(spark: SparkSession, sql: str) -> DataFrame:
    try:
        return spark.sql(sql)
    except Exception as e:
        print(f"{Print.ERROR}spark_sql_to_df failed: {e}\nSQL: {sql}")
        raise

def spark_df_to_rows(df: DataFrame) -> List[dict]:
    try:
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"{Print.ERROR}spark_df_to_rows failed: {e}")
        raise

# -------- UC identity & FQN helpers --------

def is_view(table_type: Optional[str]) -> bool:
    t = (table_type or "").upper()
    return t in {"VIEW", "MATERIALIZED_VIEW"}

def is_fully_qualified_table_name(name: str) -> bool:
    return isinstance(name, str) and name.count(".") == 2

def parse_fully_qualified_table_name(fq_table: str) -> Tuple[str, str, str]:
    if not isinstance(fq_table, str):
        print(f"{Print.ERROR}fq_table must be a string, got {type(fq_table).__name__}")
        raise TypeError("fq_table must be a string.")
    parts = fq_table.split(".")
    if len(parts) != 3:
        print(f"{Print.ERROR}Expected catalog.schema.table, got: {fq_table!r}")
        raise ValueError("Expected catalog.schema.table format.")
    return parts[0], parts[1], parts[2]

def parse_catalog_schema_fqn(schema_fqn: str) -> Tuple[str, str]:
    if not isinstance(schema_fqn, str):
        print(f"{Print.ERROR}schema_fqn must be a string, got {type(schema_fqn).__name__}")
        raise TypeError("schema_fqn must be a string.")
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        print(f"{Print.ERROR}Expected 'catalog.schema', got: {schema_fqn!r}")
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
