Damn you you dont understand at all


So here is what you should run:
ALTER TABLE {fq} ADD PRIMARY KEY ({cols})

def _handle_post_create(self, add_section):
        for key, val in add_section.items():
            if key in ("columns", "table_properties", "table_comment", "partitioned_by"):
                continue
            meta = LOADER_CONFIG["add"].get(key)
            if not meta or not val:
                continue
            sql_template = meta.get("sql")
            if sql_template is None:
                continue  # Only columns handled separately, rest should all have sql
            if key == "primary_key" or key == "partitioned_by":
                sql = sql_template.format(fq=self.fq, cols=", ".join(val))
                self._run(sql, meta["desc"].format(cols=", ".join(val)))
            elif key == "unique_keys":
                for group in val:
                    sql = sql_template.format(fq=self.fq, key="_".join(group), cols=", ".join(group))
                    self._run(sql, meta["desc"].format(cols=", ".join(group), key="_".join(group)))
            elif key == "foreign_keys":
                for fk_name, fk in val.items():
                    sql = sql_template.format(
                        fq=self.fq,
                        name=fk_name,
                        cols=", ".join(fk.get("columns", [])),
                        ref_tbl=fk.get("reference_table", ""),
                        ref_cols=", ".join(fk.get("reference_columns", [])),
                    )
                    self._run(sql, meta["desc"].format(name=fk_name, cols=", ".join(fk.get("columns", []))))
            elif key == "table_check_constraints":
                for cname, cdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=cname, expression=cdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=cname))
            elif key == "row_filters":
                for fname, fdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=fname, expression=fdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=fname))
            elif key == "table_tags":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "owner":
                sql = sql_template.format(fq=self.fq, owner=val)
                self._run(sql, meta["desc"].format(owner=val))
            # No table_comment/table_properties/columns here

    def _handle_section(self, action: str, section_dict: Dict[str, Any]):
        config = LOADER_CONFIG[action]
        for key, meta in config.items():
            val = section_dict.get(key)
            if not val:
                continue
            sql_template = meta.get("sql")
            if sql_template is None:
                self._handle_columns(action, val)
                continue
            if key == "primary_key" or key == "partitioned_by":
                sql = sql_template.format(fq=self.fq, cols=", ".join(val))
                self._run(sql, meta["desc"].format(cols=", ".join(val)))
            elif key == "unique_keys":
                for group in val:
                    sql = sql_template.format(fq=self.fq, key="_".join(group), cols=", ".join(group))
                    self._run(sql, meta["desc"].format(cols=", ".join(group), key="_".join(group)))
            elif key == "foreign_keys":
                for fk_name, fk in val.items():
                    sql = sql_template.format(
                        fq=self.fq,
                        name=fk_name,
                        cols=", ".join(fk.get("columns", [])),
                        ref_tbl=fk.get("reference_table", ""),
                        ref_cols=", ".join(fk.get("reference_columns", [])),
                    )
                    self._run(sql, meta["desc"].format(name=fk_name, cols=", ".join(fk.get("columns", []))))
            elif key == "table_check_constraints":
                for cname, cdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=cname, expression=cdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=cname))
            elif key == "row_filters":
                for fname, fdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=fname, expression=fdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=fname))
            elif key == "table_tags":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "table_properties":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "owner":
                sql = sql_template.format(fq=self.fq, owner=val)
                self._run(sql, meta["desc"].format(owner=val))
            elif key == "table_comment":
                sql = sql_template.format(fq=self.fq, comment=val)
                self._run(sql, meta["desc"])
            else:
                sql = sql_template.format(fq=self.fq, val=val)
                self._run(sql, f"{action.upper()} {key}: {val}")




Now the above does it how i want but its a lot more than what we need, 

we jsut need one function that craetes or sets the primary key

it should expect a table name and column




Can you update the table.py, the function in add_primary_key_constraint: 
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

def add_primary_key_constraint(
    spark: SparkSession,
    table_fqn: str,
    column: str = "check_id",
    constraint_name: Optional[str] = None,
    rely: bool = True,
) -> None:
    """
    Add a PRIMARY KEY constraint once. No drop/re-add logic here.
    """
    qtable = ".".join(f"`{p}`" for p in table_fqn.split("."))
    name = constraint_name or f"pk_{column}"
    opt_rely = " RELY" if rely else ""
    try:
        spark.sql(f"ALTER TABLE {qtable} ADD CONSTRAINT {name} PRIMARY KEY (`{column}`){opt_rely}")
    except Exception as e:
        print(f"{Console.WARN}Skipped PRIMARY KEY add on create for {table_fqn}.{column}: {e}")
