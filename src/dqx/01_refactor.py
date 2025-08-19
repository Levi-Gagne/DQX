Running the main i now get this: 
ModuleNotFoundError: No module named 'utils.printer'
---------------------------------------------------------------------------
ModuleNotFoundError                       Traceback (most recent call last)
File <command-4730582016129069>, line 16
     14 from utils.color import Color
     15 from utils.config import ProjectConfig
---> 16 from utils.table import (
     17     table_exists,
     18     add_primary_key_constraint,
     19     write_empty_delta_table,
     20     empty_df_from_schema,
     21 )
     23 from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED
     25 try:

File /Workspace/Users/levi.gagne@claconnect.com/DQX/src/dqx/utils/table.py:8
      6 from pyspark.sql import SparkSession, DataFrame, types as T
      7 from typing import Any
----> 8 from utils.printer import Print
     10 __all__ = [
     11     "table_exists", "refresh_table", "spark_sql_to_df", "spark_df_to_rows",
     12     "is_view",
   (...)
     18     "ensure_fully_qualified",
     19 ]
     21 # -------- Spark helpers --------

ModuleNotFoundError: No module named 'utils.printer'


Here is the table:
# src/utils/table.py

from __future__ import annotations

from typing import Tuple, List, Optional
from pyspark.sql import SparkSession, DataFrame, types as T
from typing import Any
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
        print(f"{Print.WARN}Skipped PRIMARY KEY add on create for {table_fqn}.{column}: {e}")




So, I have renamed the printer called here to:
# src/dqx/utils/console.py

from __future__ import annotations

import os
import sys
import time as _time
import re
from typing import Dict, List, Optional, Iterable
from functools import wraps

from utils.color import Color


class Console:
    """
    Color tokens, banners, and a timer decorator.

    Usage:
        print(f"{Console.INFO}Starting…")
        Console.print_banner("h2", "Loading rules")

        @Console.banner_timer("Run DQX Checks", kind="h1")
        def run(): ...
    """

    # enable colors iff NOT disabled and TTY likely present
    _ENABLED = (os.environ.get("NO_COLOR", "") == "") and bool(getattr(sys.stdout, "isatty", lambda: False)())
    _RESET   = getattr(Color, "r", "") if _ENABLED else ""

    # ----- internal helpers -----
    @classmethod
    def _code(cls, name: str) -> str:
        return getattr(Color, name, "") if cls._ENABLED else ""

    @classmethod
    def _codes(cls, names: Iterable[str]) -> str:
        if not cls._ENABLED:
            return ""
        return "".join(getattr(Color, n, "") for n in names if isinstance(n, str))

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        s = float(seconds)
        m, s = divmod(s, 60.0)
        h, m = divmod(int(m), 60)
        return f"{h}h {m}m {s:.2f}s" if h else (f"{m}m {s:.2f}s" if m else f"{s:.2f}s")

    # ----- tokens -----
    BRACKET_STYLE: List[str] = ["b", "sky_blue"]
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      ["b", "candy_red", "r"],
        "WARNING":    ["b", "yellow", "r"],
        "WARN":       ["b", "yellow", "r"],
        "INFO":       ["b", "sky_blue", "r"],
        "SUCCESS":    ["b", "green", "r"],
        "VALIDATION": ["b", "ivory", "r"],
        "DEBUG":      ["r"],
    }

    @classmethod
    def token(cls, label: str) -> str:
        lab = (label or "").upper()
        br  = cls._codes(cls.BRACKET_STYLE)
        lb  = cls._codes(cls.TAG_STYLES.get(lab, ["r"]))
        return f"{br}[{cls._RESET}{lb}{lab}{cls._RESET}{br}]{cls._RESET}"

    _TOKEN = re.compile(r"\{([A-Za-z0-9_]+)\}")
    @classmethod
    def tagify(cls, s: str) -> str:
        return cls._TOKEN.sub(lambda m: cls.token(m.group(1)), s)

    # ----- banners -----
    BANNERS: Dict[str, Dict[str, object]] = {
        "app": {"width": 72, "border": "sky_blue",  "text": "ivory"},
        "h1":  {"width": 72, "border": "aqua_blue", "text": "ivory"},
        "h2":  {"width": 64, "border": "green",     "text": "ivory"},
        "h3":  {"width": 56, "border": "sky_blue",  "text": "ivory"},
        "h4":  {"width": 48, "border": "aqua_blue", "text": "ivory"},
        "h5":  {"width": 36, "border": "green",     "text": "ivory"},
        "h6":  {"width": 24, "border": "ivory",     "text": "sky_blue"},
    }

    @classmethod
    def banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> str:
        k = kind.lower()
        cfg = cls.BANNERS.get(k)
        if not cfg:
            raise KeyError(f"Unknown banner kind '{kind}'. Known: {sorted(cls.BANNERS)}")
        w = int(width or cfg["width"])
        border_attr = str((shades or {}).get("border", cfg["border"]))
        text_attr   = str((shades or {}).get("text",   cfg["text"]))

        border = cls._code(border_attr)
        text   = cls._code(text_attr)

        bar = f"{border}{Color.b}" + "═" * w + cls._RESET
        inner_width = max(0, w - 4)
        title_line = (
            f"{border}{Color.b}║ "
            f"{text}{Color.b}{title.center(inner_width)}{cls._RESET}"
            f"{border}{Color.b} ║{cls._RESET}"
        )
        return f"\n{bar}\n{title_line}\n{bar}"

    @classmethod
    def print_banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> None:
        print(cls.banner(kind, title, shades=shades, width=width))

    @classmethod
    def banner_timer(cls, title: Optional[str] = None, *, kind: str = "app", shades: Optional[Dict[str, str]] = None):
        """Decorator that prints START/END banners with elapsed time."""
        def deco(fn):
            text = title or fn.__name__.replace("_", " ").title()
            @wraps(fn)
            def wrapped(*args, **kwargs):
                print(cls.banner(kind, f"START {text}", shades=shades))
                t0 = _time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = _time.perf_counter() - t0
                    print(cls.banner(kind, f"END {text} — finished in {cls._format_elapsed(elapsed)}", shades=shades))
            return wrapped
        return deco


# export convenience constants like Console.ERROR / Console.INFO
for _lab in list(Console.TAG_STYLES):
    setattr(Console, _lab, Console.token(_lab))





So can you update my table.py so it uses the new console please
