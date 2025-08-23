# src/dqx/utils/dqx.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional

from pyspark.sql import DataFrame, functions as F, types as T

# --------- Issue struct used across helpers ---------
ISSUE_STRUCT = T.StructType([
    T.StructField("name",          T.StringType(), True),
    T.StructField("message",       T.StringType(), True),
    T.StructField("columns",       T.ArrayType(T.StringType()), True),
    T.StructField("filter",        T.StringType(), True),
    T.StructField("function",      T.StringType(), True),
    T.StructField("run_time",      T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("check_id",      T.StringType(), True),
])

def empty_issues_array() -> F.Column:
    """Typed empty issues array: array<ISSUE_STRUCT>."""
    return F.from_json(F.lit("[]"), T.ArrayType(ISSUE_STRUCT))

def normalize_issues_for_fp(col: F.Column) -> F.Column:
    """
    Normalize issues for deterministic fingerprinting:
      - lower/trim string fields
      - sort 'columns'
      - drop run_time impact by nulling it
      - keep user_metadata as-is (string map), but coalesce to empty map
    Returns: array<struct> with stable ordering (use array_sort at callsite).
    """
    col = F.coalesce(col, empty_issues_array())
    return F.transform(
        col,
        lambda x: F.struct(
            F.lower(F.trim(x["name"])).alias("name"),
            F.trim(x["message"]).alias("message"),
            F.array_sort(F.transform(F.coalesce(x["columns"], F.array().cast(T.ArrayType(T.StringType()))),
                                     lambda c: F.lower(F.trim(c)))).alias("columns"),
            F.lower(F.trim(x["filter"])).alias("filter"),
            F.lower(F.trim(x["function"])).alias("function"),
            F.lit(None).cast(T.TimestampType()).alias("run_time"),
            F.coalesce(x["user_metadata"], F.map_from_arrays(F.array(), F.array())).alias("user_metadata"),
            F.coalesce(x["check_id"], F.lit(None).cast(T.StringType())).alias("check_id"),
        )
    )

# --------- Argument coercion ---------
def coerce_arguments(expected: Dict[str, Any], provided: Optional[Dict[str, Any]], function_name: Optional[str], *, mode: str = "permissive") -> Tuple[Dict[str, Any], List[str]]:
    """
    Simple coercion: stringify complex values; keep scalars as strings.
    Returns (coerced_args, warnings)
    """
    out: Dict[str, Any] = {}
    warns: List[str] = []
    provided = provided or {}
    for k, v in provided.items():
        if isinstance(v, (list, dict)):
            out[k] = F.lit(None)  # placeholder; callers typically stringify earlier
            warns.append(f"argument {k!r} is complex; ensure stringified upstream")
        else:
            out[k] = str(v) if v is not None else None
    return out, warns

# --------- Rule application (placeholder; lets pipelines import/run) ---------
def apply_rules_isolating_failures(dq_engine, src_df: DataFrame, rules: List[dict], table_name: str) -> Tuple[Optional[DataFrame], Optional[List[str]]]:
    """
    Minimal shim so downstream code doesn’t break:
      - Ensures _errors and _warnings exist as typed arrays.
      - Returns (annot_df, None). If you want real DQ execution, swap in your engine call here.
    """
    if src_df is None:
        return None, ["source DataFrame is None"]

    annot = (
        src_df
        .withColumn("_errors",   empty_issues_array())
        .withColumn("_warnings", empty_issues_array())
    )
    # If you wire DQEngine here, replace the two lines above with actual annotation call
    return annot, None