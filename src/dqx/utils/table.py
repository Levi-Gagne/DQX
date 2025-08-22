# src/utils/table.py
from __future__ import annotations

from typing import Tuple, List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame, types as T

from utils.console import Console
from utils.documentation import apply_table_documentation

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
    "create_table_if_absent",
    # NEW reusable helpers:
    "dtype_to_string",
    "struct_to_columns_spec",
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

# -------- NEW: StructType -> columns spec (for TableWriter & comments) --------

def dtype_to_string(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType):     return "string"
    if isinstance(dt, T.BooleanType):    return "boolean"
    if isinstance(dt, T.TimestampType):  return "timestamp"
    if isinstance(dt, T.DateType):       return "date"
    if isinstance(dt, T.LongType):       return "long"
    if isinstance(dt, T.IntegerType):    return "integer"
    if isinstance(dt, T.ShortType):      return "short"
    if isinstance(dt, T.ByteType):       return "byte"
    if isinstance(dt, T.FloatType):      return "float"
    if isinstance(dt, T.DoubleType):     return "double"
    if isinstance(dt, T.DecimalType):    return "decimal"
    if isinstance(dt, T.ArrayType):      return "array"
    if isinstance(dt, T.MapType):        return "map"
    if isinstance(dt, T.StructType):     return "struct"
    # Fallback to repr for exotic types
    return str(dt)

def struct_to_columns_spec(st: T.StructType, comments: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Convert a Spark StructType (with nullability) into a TableWriter-friendly columns spec:
    - Keeps name, nullability
    - Emits `data_type` as a simple string for portability
    - Carries comments from (1) explicit `comments` map (highest precedence) or
      (2) field.metadata["comment"] if present.
    - Recurses for struct/array/map.
    """
    comments = comments or {}

    def field_to_spec(sf: T.StructField, parent: Optional[str] = None) -> Dict[str, Any]:
        fq = f"{parent}.{sf.name}" if parent else sf.name
        spec: Dict[str, Any] = {
            "name": sf.name,
            "data_type": dtype_to_string(sf.dataType),
            "nullable": bool(sf.nullable),
        }
        # comment precedence: external map > field.metadata.comment
        c = comments.get(fq)
        if not c and sf.metadata and "comment" in sf.metadata:
            c = sf.metadata["comment"]
        if c:
            spec["comment"] = c

        dt = sf.dataType
        if isinstance(dt, T.StructType):
            spec["fields"] = [field_to_spec(ch, fq) for ch in dt.fields]
        elif isinstance(dt, T.ArrayType):
            elem = dt.elementType
            elem_spec: Dict[str, Any] = {"type": dtype_to_string(elem)}
            if isinstance(elem, T.StructType):
                elem_spec["fields"] = [field_to_spec(ch, fq) for ch in elem.fields]
            spec["element"] = elem_spec
        elif isinstance(dt, T.MapType):
            spec["key_type"] = dtype_to_string(dt.keyType)
            spec["value_type"] = dtype_to_string(dt.valueType)
        return spec

    return [field_to_spec(f) for f in st.fields]