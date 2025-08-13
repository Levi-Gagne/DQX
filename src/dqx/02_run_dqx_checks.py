# Databricks notebook source
# MAGIC %md
# MAGIC # Run DQX Checks

# COMMAND ----------

# DBTITLE 1,Install DQX Package
# MAGIC %pip install databricks-labs-dqx==0.8.0

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
dbutils.library.restartPython()

# COMMAND ----------

# Databricks notebook: 02_run_dqx_checks
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

import json
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

# -------------------
# Config / IO helper
# -------------------
def read_yaml(path: str) -> Dict[str, Any]:
    p = path.replace("dbfs:/", "/dbfs/") if path.startswith("dbfs:/") else path
    with open(p, "r") as fh:
        return yaml.safe_load(fh) or {}

# -------------
# Result schema
# -------------
ROW_LOG_SCHEMA = T.StructType([
    T.StructField("log_id",                      T.StringType(),  False),  # PK (deterministic)
    T.StructField("check_id",                    T.ArrayType(T.StringType()), True),  # backrefs for all issues on the row
    T.StructField("table_name",                  T.StringType(),  False),
    T.StructField("run_config_name",             T.StringType(),  False),

    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",                   T.StringType(), True),
        T.StructField("message",                T.StringType(), True),
        T.StructField("columns",                T.ArrayType(T.StringType()), True),
        T.StructField("filter",                 T.StringType(), True),
        T.StructField("function",               T.StringType(), True),
        T.StructField("run_time",               T.TimestampType(), True),
        T.StructField("user_metadata",          T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_errors_fingerprint",         T.StringType(),  False),

    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",                   T.StringType(), True),
        T.StructField("message",                T.StringType(), True),
        T.StructField("columns",                T.ArrayType(T.StringType()), True),
        T.StructField("filter",                 T.StringType(), True),
        T.StructField("function",               T.StringType(), True),
        T.StructField("run_time",               T.TimestampType(), True),
        T.StructField("user_metadata",          T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_warnings_fingerprint",       T.StringType(),  False),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",                 T.StringType(), False),
        T.StructField("value",                  T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint",    T.StringType(),  False),

    T.StructField("created_by",                  T.StringType(),  False),
    T.StructField("created_at",                  T.TimestampType(), False),
    T.StructField("updated_by",                  T.StringType(),  True),
    T.StructField("updated_at",                  T.TimestampType(), True),
])

def ensure_table(full_name: str):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    if not spark.catalog.tableExists(full_name):
        cat, sch, _ = full_name.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        spark.createDataFrame([], ROW_LOG_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(full_name)

# --------------------------
# Issue array helpers
# --------------------------
def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _empty_issues_array() -> F.Column:
    elem = T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    ])
    return F.from_json(F.lit("[]"), T.ArrayType(elem))

def _normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # stable fp: sort 'columns'; drop volatile bits
    return F.transform(
        arr_col,
        lambda r: F.struct(
            r["name"].alias("name"),
            r["message"].alias("message"),
            F.coalesce(F.to_json(F.array_sort(r["columns"])), F.lit("[]")).alias("columns_json"),
            r["filter"].alias("filter"),
            r["function"].alias("function"),
        ),
    )

# --------------------------
# JIT argument coercion (exec-time only)
# --------------------------
_EXPECTED: Dict[str, Dict[str, str]] = {
    "is_unique": {"columns": "list"},
    "is_in_list": {"column": "str", "allowed": "list"},
    "is_in_range": {"column": "str", "min_limit": "num", "max_limit": "num",
                    "inclusive_min": "bool", "inclusive_max": "bool"},
    "regex_match": {"column": "str", "regex": "str"},
    "sql_expression": {"expression": "str"},
    "sql_query": {"query": "str", "limit": "num"},   # if you ever use sql_query, require limit
    "is_not_null": {"column": "str"},
    "is_not_null_and_not_empty": {"column": "str"},  # often with for_each_column
}

def _parse_scalar(s: Optional[str]):
    if s is None: return None
    s = s.strip()
    sl = s.lower()
    if sl in ("null", "none", ""): return None
    if sl == "true": return True
    if sl == "false": return False
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try: return json.loads(s)
        except Exception: return s
    try:
        return int(s) if s.lstrip("+-").isdigit() else float(s)
    except Exception:
        return s

def _to_list(v):
    if v is None: return []
    if isinstance(v, list): return v
    if isinstance(v, str) and v.strip().startswith("["):
        try: return json.loads(v)
        except Exception: return [v]
    return [v]

def _to_num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return v
    try: return int(v) if str(v).lstrip("+-").isdigit() else float(v)
    except Exception: return v

def _to_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str):
        vl = v.strip().lower()
        if vl in ("true", "t", "1"): return True
        if vl in ("false", "f", "0"): return False
    return v

def _coerce_arguments(args_map: Optional[Dict[str, str]],
                      function_name: Optional[str],
                      mode: str = "permissive") -> Tuple[Dict[str, Any], List[str]]:
    """
    Coerce map<string,string> -> properly typed dict for DQX. Exec-time only.
    Returns (coerced_args, errors)
    """
    if not args_map: return {}, []
    raw = {k: _parse_scalar(v) for k, v in args_map.items()}
    spec = _EXPECTED.get((function_name or "").strip(), {})

    out: Dict[str, Any] = {}
    errs: List[str] = []

    for k, v in raw.items():
        want = spec.get(k)
        if want == "list":
            out[k] = _to_list(v)
            if not isinstance(out[k], list):
                errs.append(f"key '{k}' expected list, got {type(out[k]).__name__}")
        elif want == "num":
            out[k] = _to_num(v)
        elif want == "bool":
            out[k] = _to_bool(v)
        elif want == "str":
            out[k] = "" if v is None else str(v)
        else:
            out[k] = v  # keep parsed scalar (unknown key for this fn)

    if (function_name or "").strip() == "sql_query" and ("limit" not in out or out.get("limit") in (None, 0)):
        errs.append("sql_query requires a positive 'limit'")

    if mode == "strict" and errs:
        raise ValueError(f"Argument coercion failed for '{function_name}': {errs}")
    return out, errs

# --------------------------
# Load rules from the table (as plain dicts, JIT-coerced)
# --------------------------
def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for r in rules:
        out.setdefault(r["table_name"], []).append(r)
    return out

def _load_checks_from_table_as_dicts(spark: SparkSession,
                                     checks_table: str,
                                     run_config_name: str,
                                     coercion_mode: str = "permissive") -> Tuple[Dict[str, List[dict]], int, int]:
    """
    Return {table_name: [rule_dict, ...]} using the exact dict structure DQX expects.
    Also returns (coerced_count, skipped_count) where skipped are rules failing DQX validation even after coercion.
    """
    df = (
        spark.table(checks_table)
        .where((F.col("run_config_name") == run_config_name) & (F.col("active") == True))
        .select("table_name", "name", "criticality", "filter",
                "run_config_name", "user_metadata", "check")
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]

    raw_rules: List[dict] = []
    coerced: int = 0

    for r in rows:
        chk = r.get("check") or {}
        fn  = chk.get("function")
        fec = chk.get("for_each_column")  # already array<string>
        args, _errs = _coerce_arguments(chk.get("arguments"), fn, mode=coercion_mode)
        coerced += 1

        raw_rules.append({
            "table_name":       r["table_name"],
            "name":             r["name"],
            "criticality":      r["criticality"],
            "run_config_name":  r["run_config_name"],
            "filter":           r.get("filter"),
            "user_metadata":    r.get("user_metadata"),
            "check": {
                "function":        fn,
                "for_each_column": fec if fec else None,
                "arguments":       args,
            },
        })

    # DQX validate; keep only valid rules (permissive), or raise (strict)
    status = DQEngine.validate_checks(raw_rules)
    if getattr(status, "has_errors", False):
        keep: List[dict] = []
        skipped: List[str] = []
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if getattr(st, "has_errors", False):
                skipped.append(r.get("name") or "<unnamed>")
            else:
                keep.append(r)
        return _group_by_table(keep), coerced, len(skipped)
    else:
        return _group_by_table(raw_rules), coerced, 0

# --------------------------
# Apply with isolation and diagnostics
# --------------------------
def _apply_rules_isolating_failures(dq: DQEngine,
                                    src: DataFrame,
                                    table_name: str,
                                    tbl_rules: List[dict]) -> Tuple[Optional[DataFrame], List[Tuple[str, str]]]:
    """
    Try applying all rules. If it fails, try each rule individually to identify the bad ones.
    Returns (annotated_df_or_None, list_of_(rule_name, error)).
    Skips the failing rules and applies remaining if possible.
    """
    try:
        return dq.apply_checks_by_metadata(src, tbl_rules), []
    except Exception:
        bad: List[Tuple[str, str]] = []
        good: List[dict] = []
        for r in tbl_rules:
            try:
                dq.apply_checks_by_metadata(src, [r])
                good.append(r)
            except Exception as ex:
                bad.append((r.get("name") or "<unnamed>", str(ex)))
                # dump the offending rule for quick inspection
                try:
                    print(f"    offending rule JSON: {json.dumps(r, indent=2)}")
                except Exception:
                    pass

        if bad:
            print(f"[{table_name}] Skipping {len(bad)} bad rule(s):")
            for nm, err in bad:
                print(f"  - {nm}: {err}")
        if not good:
            return None, bad

        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception as ex2:
            print(f"[{table_name}] Still failing after pruning bad rules: {ex2}")
            return None, bad

# --------------------------
# Projection & enrichment
# --------------------------
def _project_row_hits(df_annot: DataFrame,
                      table_name: str,
                      run_config_name: str,
                      created_by: str,
                      exclude_cols: Optional[List[str]] = None) -> DataFrame:
    exclude_cols = set(exclude_cols or [])

    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errors_col   = F.col(e_name) if e_name else _empty_issues_array()
    warnings_col = F.col(w_name) if w_name else _empty_issues_array()

    df = (df_annot
          .withColumn("_errs", errors_col)
          .withColumn("_warns", warnings_col)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]
    row_snapshot = F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value")) for c in sorted(cols)])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_warns")))), 256)

    log_id = F.sha2(F.concat_ws("||",
                                F.lit(table_name),
                                F.lit(run_config_name),
                                row_snapshot_fp,
                                _errors_fp,
                                _warnings_fp), 256)

    return df.select(
        log_id.alias("log_id"),
        F.lit(None).cast(T.ArrayType(T.StringType())).alias("check_id"),  # fill later
        F.lit(table_name).alias("table_name"),
        F.lit(run_config_name).alias("run_config_name"),
        F.col("_errs").alias("_errors"),
        _errors_fp.alias("_errors_fingerprint"),
        F.col("_warns").alias("_warnings"),
        _warnings_fp.alias("_warnings_fingerprint"),
        row_snapshot.alias("row_snapshot"),
        row_snapshot_fp.alias("row_snapshot_fingerprint"),
        F.lit(created_by).alias("created_by"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast(T.StringType()).alias("updated_by"),
        F.lit(None).cast(T.TimestampType()).alias("updated_at"),
    )

def _enrich_check_ids(row_log_df: DataFrame, checks_table: str) -> DataFrame:
    # Spark Connect-safe: don't access df.sql_ctx / df.sparkSession
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    chk = spark.table(checks_table).select(
        F.col("table_name").alias("t_tbl"),
        F.col("run_config_name").alias("t_rc"),
        F.col("name").alias("t_name"),
        F.col("check_id").alias("cfg_check_id"),
        F.col("active").alias("t_active")
    )

    names = (
        row_log_df
        .select("log_id", "table_name", "run_config_name",
                F.expr("transform(_errors, x -> x.name)").alias("e_names"),
                F.expr("transform(_warnings, x -> x.name)").alias("w_names"))
        .withColumn("all_names", F.array_union("e_names", "w_names"))
        .withColumn("name", F.explode_outer("all_names"))
        .drop("e_names","w_names","all_names")
    )

    j = (
        names.join(
            chk,
            (names.table_name == chk.t_tbl) &
            (names.run_config_name == chk.t_rc) &
            (names.name == chk.t_name) &
            (chk.t_active == F.lit(True)),
            "left"
        )
        .groupBy("log_id")
        .agg(F.array_sort(F.array_distinct(F.collect_list("cfg_check_id"))).alias("check_id"))
    )

    return (
        row_log_df.drop("check_id")
        .join(j, "log_id", "left")
        .withColumn("check_id", F.expr("filter(check_id, x -> x is not null)"))
    )

# --------------------------
# Summaries (Connect-safe, no overflow)
# --------------------------
def _print_summary_for_tbl(tbl: str, annot: DataFrame):
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    agg = (
        annot.select(
            F.size(F.col(err)).cast("long").alias("e_sz"),
            F.size(F.col(wrn)).cast("long").alias("w_sz"),
        )
        .agg(
            F.coalesce(F.sum("e_sz"), F.lit(0).cast("long")).alias("errors"),
            F.coalesce(F.sum("w_sz"), F.lit(0).cast("long")).alias("warnings"),
        )
        .collect()[0]
    )
    print(f"[{tbl}] hits -> errors={int(agg['errors'])}, warnings={int(agg['warnings'])}")

    by_rule = (
        annot
        .select(
            F.explode_outer(
                F.array_union(
                    F.expr(f"transform({err}, x -> struct(x.name as name, 'error' as severity))"),
                    F.expr(f"transform({wrn}, x -> struct(x.name as name, 'warning' as severity))"),
                )
            ).alias("h")
        )
        .where(F.col("h.name").isNotNull())
        .select("h.name", "h.severity")
        .groupBy("name")
        .count()
        .orderBy(F.desc("count"), F.asc("name"))
    )
    print("Top rule hits:")
    by_rule.show(20, truncate=False)

def _df_has_rows(df: DataFrame) -> bool:
    return df.limit(1).count() > 0

# ---------------
# Main entry point
# ---------------
def run_checks(
    dqx_cfg_yaml: str,
    created_by: str = "AdminUser",
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive"  # or "strict"
):
    spark = SparkSession.builder.getOrCreate()
    dq = DQEngine(WorkspaceClient())

    cfg = read_yaml(dqx_cfg_yaml)
    checks_table  = cfg["dqx_checks_config_table_name"]   # e.g., dq_dev.dqx.checks_config
    results_table = cfg["dqx_checks_log_table_name"]      # e.g., dq_dev.dqx.checks_log
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}

    ensure_table(results_table)

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    for rc_name, rc_cfg in rc_map.items():
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        write_mode = (rc_cfg or {}).get("output_config", {}).get("mode", "overwrite")
        write_opts = (rc_cfg or {}).get("output_config", {}).get("options", {}) or {}

        by_tbl, coerced, skipped = _load_checks_from_table_as_dicts(
            spark, checks_table, rc_name, coercion_mode=coercion_mode
        )
        checks_loaded = sum(len(v) for v in by_tbl.values())
        print(f"[{rc_name}] checks_in_table_total={checks_table_total}, loaded={checks_loaded}, coerced={coerced}, skipped_invalid={skipped}")

        if not checks_loaded:
            print(f"[{rc_name}] no checks loaded (active=TRUE & run_config_name='{rc_name}').")
            continue

        out_batches: List[DataFrame] = []
        for tbl, tbl_rules in by_tbl.items():
            try:
                src = spark.read.table(tbl)
                annot, bad = _apply_rules_isolating_failures(dq, src, tbl, tbl_rules)
                if annot is None:
                    continue  # nothing runnable for this table
            except Exception as e:
                print(f"[{rc_name}] {tbl} failed: {e}")
                continue

            _print_summary_for_tbl(tbl, annot)
            row_hits = _project_row_hits(annot, tbl, rc_name, created_by, exclude_cols=exclude_cols)
            if _df_has_rows(row_hits):
                out_batches.append(row_hits)

        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = out_batches[0]
        for b in out_batches[1:]:
            out = out.unionByName(b, allowMissingColumns=True)

        out = _enrich_check_ids(out, checks_table)

        out = out.select([f.name for f in ROW_LOG_SCHEMA.fields])
        rows = out.count()
        out.write.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_table}")

    print(f"TOTAL rows written: {grand_total}")


# ---- run it ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"],
    coercion_mode="strict"   # 'strict'| 'permissive' – strict will hard-fail on any invalid rule args
)

# COMMAND ----------

# Databricks notebook: 02_run_dqx_checks
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

import json
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T
from utils.color import Color 

# -------------------
# Display helpers (for nice notebook output)
# -------------------
def _can_display() -> bool:
    return "display" in globals()

def show_df(df: DataFrame, n: int = 100, truncate: bool = False) -> None:
    if _can_display():
        display(df.limit(n))
    else:
        df.show(n, truncate=truncate)

def display_section(title: str) -> None:
    print("\n" + "═" * 80)
    print(f"║ {title}")
    print("═" * 80)

# -------------------
# Config / IO helper
# -------------------
def read_yaml(path: str) -> Dict[str, Any]:
    p = path.replace("dbfs:/", "/dbfs/") if path.startswith("dbfs:/") else path
    with open(p, "r") as fh:
        return yaml.safe_load(fh) or {}

# -------------
# Result schema
# -------------
ROW_LOG_SCHEMA = T.StructType([
    T.StructField("log_id",                      T.StringType(),  False),  # PK (deterministic)
    T.StructField("check_id",                    T.ArrayType(T.StringType()), True),  # originating rule ids
    T.StructField("table_name",                  T.StringType(),  False),
    T.StructField("run_config_name",             T.StringType(),  False),

    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",                   T.StringType(), True),
        T.StructField("message",                T.StringType(), True),
        T.StructField("columns",                T.ArrayType(T.StringType()), True),
        T.StructField("filter",                 T.StringType(), True),
        T.StructField("function",               T.StringType(), True),
        T.StructField("run_time",               T.TimestampType(), True),
        T.StructField("user_metadata",          T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_errors_fingerprint",         T.StringType(),  False),

    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",                   T.StringType(), True),
        T.StructField("message",                T.StringType(), True),
        T.StructField("columns",                T.ArrayType(T.StringType()), True),
        T.StructField("filter",                 T.StringType(), True),
        T.StructField("function",               T.StringType(), True),
        T.StructField("run_time",               T.TimestampType(), True),
        T.StructField("user_metadata",          T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_warnings_fingerprint",       T.StringType(),  False),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",                 T.StringType(), False),
        T.StructField("value",                  T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint",    T.StringType(),  False),

    T.StructField("created_by",                  T.StringType(),  False),
    T.StructField("created_at",                  T.TimestampType(), False),
    T.StructField("updated_by",                  T.StringType(),  True),
    T.StructField("updated_at",                  T.TimestampType(), True),
])

# -------------------
# Documentation dictionary (table + columns) for results table
# -------------------
DQX_CHECKS_LOG_METADATA: Dict[str, Any] = {
    "table": "dq_dev.dqx.checks_log",  # override with your actual FQN at create time
    "table_comment": (
        "# DQX Row-level Check Results Log\n"
        "- One row per source row that triggered at least one rule (error or warning).\n"
        "- `check_id` contains the originating rule IDs from the config table.\n"
        "- Fingerprint columns are deterministic digests to aid de-duplication & rollups.\n"
    ),
    "columns": {
        "log_id": (
            "Deterministic SHA-256 of the tuple "
            "(table_name, run_config_name, row_snapshot_fingerprint, "
            "_errors_fingerprint, _warnings_fingerprint)."
        ),
        "check_id": (
            "Array of originating rule IDs (from config.check_id) that fired for this row. "
            "May be empty if a rule-name mapping was not found."
        ),
        "table_name": "Fully qualified source table (`catalog.schema.table`).",
        "run_config_name": "Run configuration tag/group under which checks were applied.",

        "_errors": (
            "Array<struct> of error issues. Fields: "
            "{name, message, columns, filter, function, run_time, user_metadata}."
        ),
        "_errors_fingerprint": "SHA-256 of a normalized view of `_errors` (stable across column-order changes).",

        "_warnings": (
            "Array<struct> of warning issues. Fields: "
            "{name, message, columns, filter, function, run_time, user_metadata}."
        ),
        "_warnings_fingerprint": "SHA-256 of a normalized view of `_warnings`.",

        "row_snapshot": (
            "Array<struct{column:string, value:string}> capturing the non-reserved columns "
            "for the offending row at evaluation time (values stringified)."
        ),
        "row_snapshot_fingerprint": "SHA-256 of JSON(row_snapshot).",

        "created_by": "Audit: user or process that wrote this record.",
        "created_at": "Audit: creation timestamp (UTC).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (UTC, nullable).",
    },
}

# -------------------
# Comment application helpers (safe for clusters & SQL Warehouses)
# -------------------
def _esc_sql_comment(s: str) -> str:
    return (s or "").replace("'", "''")

def _comment_on_table(spark: SparkSession, fqn: str, text: Optional[str]):
    if not text:
        return
    cat, sch, tbl = fqn.split(".")
    spark.sql(
        f"COMMENT ON TABLE `{cat}`.`{sch}`.`{tbl}` IS '{_esc_sql_comment(text)}'"
    )

def _set_column_comment_safe(spark: SparkSession, fqn: str, col_name: str, comment: str):
    """
    Try COMMENT ON COLUMN first; if parser complains, fall back to ALTER ... CHANGE COLUMN with type.
    """
    cat, sch, tbl = fqn.split(".")
    escaped = _esc_sql_comment(comment)
    try:
        spark.sql(
            f"COMMENT ON COLUMN `{cat}`.`{sch}`.`{tbl}`.`{col_name}` IS '{escaped}'"
        )
        return
    except Exception as _:
        # Fallback: need the data type
        info = spark.sql(f"DESCRIBE TABLE `{cat}`.`{sch}`.`{tbl}`").collect()
        types_map = {r.col_name: r.data_type for r in info if r.col_name and not r.col_name.startswith("#")}
        dt = types_map.get(col_name)
        if not dt:
            return
        spark.sql(
            f"ALTER TABLE `{cat}`.`{sch}`.`{tbl}` CHANGE COLUMN `{col_name}` `{col_name}` {dt} COMMENT '{escaped}'"
        )

def _apply_table_documentation_on_create(spark: SparkSession, table_fqn: str,
                                         doc: Dict[str, Any], just_created: bool):
    """
    Apply table & column comments. Table comment is set on create; column comments always attempted.
    """
    if not doc:
        return
    try:
        if just_created:
            _comment_on_table(spark, table_fqn, doc.get("table_comment"))

        cat, sch, tbl = table_fqn.split(".")
        existing = {
            r.col_name for r in spark.sql(f"DESCRIBE TABLE `{cat}`.`{sch}`.`{tbl}`").collect()
            if r.col_name and not str(r.col_name).startswith("#")
        }
        for col_name, cmt in (doc.get("columns") or {}).items():
            if col_name in existing:
                _set_column_comment_safe(spark, table_fqn, col_name, cmt)
    except Exception as e:
        print(f"[WARN] Could not apply table/column comments to {table_fqn}: {e}")

def ensure_table(full_name: str, doc: Optional[Dict[str, Any]] = None):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    existed = spark.catalog.tableExists(full_name)
    if not existed:
        cat, sch, _ = full_name.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        spark.createDataFrame([], ROW_LOG_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(full_name)
    _apply_table_documentation_on_create(spark, full_name, doc or {}, just_created=(not existed))

# --------------------------
# Issue array helpers
# --------------------------
def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _empty_issues_array() -> F.Column:
    elem = T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    ])
    return F.from_json(F.lit("[]"), T.ArrayType(elem))

def _normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # stable fp: sort 'columns'; drop volatile bits
    return F.transform(
        arr_col,
        lambda r: F.struct(
            r["name"].alias("name"),
            r["message"].alias("message"),
            F.coalesce(F.to_json(F.array_sort(r["columns"])), F.lit("[]")).alias("columns_json"),
            r["filter"].alias("filter"),
            r["function"].alias("function"),
        ),
    )

# --------------------------
# JIT argument coercion (exec-time only)
# --------------------------
_EXPECTED: Dict[str, Dict[str, str]] = {
    "is_unique": {"columns": "list"},
    "is_in_list": {"column": "str", "allowed": "list"},
    "is_in_range": {"column": "str", "min_limit": "num", "max_limit": "num",
                    "inclusive_min": "bool", "inclusive_max": "bool"},
    "regex_match": {"column": "str", "regex": "str"},
    "sql_expression": {"expression": "str"},
    "sql_query": {"query": "str", "limit": "num"},
    "is_not_null": {"column": "str"},
    "is_not_null_and_not_empty": {"column": "str"},
}

def _parse_scalar(s: Optional[str]):
    if s is None: return None
    s = s.strip()
    sl = s.lower()
    if sl in ("null", "none", ""): return None
    if sl == "true": return True
    if sl == "false": return False
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try: return json.loads(s)
        except Exception: return s
    try:
        return int(s) if s.lstrip("+-").isdigit() else float(s)
    except Exception:
        return s

def _to_list(v):
    if v is None: return []
    if isinstance(v, list): return v
    if isinstance(v, str) and v.strip().startswith("["):
        try: return json.loads(v)
        except Exception: return [v]
    return [v]

def _to_num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return v
    try: return int(v) if str(v).lstrip("+-").isdigit() else float(v)
    except Exception: return v

def _to_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str):
        vl = v.strip().lower()
        if vl in ("true", "t", "1"): return True
        if vl in ("false", "f", "0"): return False
    return v

def _coerce_arguments(args_map: Optional[Dict[str, str]],
                      function_name: Optional[str],
                      mode: str = "permissive") -> Tuple[Dict[str, Any], List[str]]:
    if not args_map: return {}, []
    raw = {k: _parse_scalar(v) for k, v in args_map.items()}
    spec = _EXPECTED.get((function_name or "").strip(), {})

    out: Dict[str, Any] = {}
    errs: List[str] = []
    for k, v in raw.items():
        want = spec.get(k)
        if want == "list":
            out[k] = _to_list(v)
            if not isinstance(out[k], list):
                errs.append(f"key '{k}' expected list, got {type(out[k]).__name__}")
        elif want == "num":
            out[k] = _to_num(v)
        elif want == "bool":
            out[k] = _to_bool(v)
        elif want == "str":
            out[k] = "" if v is None else str(v)
        else:
            out[k] = v
    if (function_name or "").strip() == "sql_query" and ("limit" not in out or out.get("limit") in (None, 0)):
        errs.append("sql_query requires a positive 'limit'")
    if mode == "strict" and errs:
        raise ValueError(f"Argument coercion failed for '{function_name}': {errs}")
    return out, errs

# --------------------------
# Load rules from the table (as plain dicts, JIT-coerced)
# --------------------------
def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for r in rules:
        out.setdefault(r["table_name"], []).append(r)
    return out

def _load_checks_from_table_as_dicts(spark: SparkSession,
                                     checks_table: str,
                                     run_config_name: str,
                                     coercion_mode: str = "permissive") -> Tuple[Dict[str, List[dict]], int, int]:
    df = (
        spark.table(checks_table)
        .where((F.col("run_config_name") == run_config_name) & (F.col("active") == True))
        .select("table_name", "name", "criticality", "filter",
                "run_config_name", "user_metadata", "check")
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]

    raw_rules: List[dict] = []
    coerced: int = 0

    for r in rows:
        chk = r.get("check") or {}
        fn  = chk.get("function")
        fec = chk.get("for_each_column")
        args, _errs = _coerce_arguments(chk.get("arguments"), fn, mode=coercion_mode)
        coerced += 1

        raw_rules.append({
            "table_name":       r["table_name"],
            "name":             r["name"],
            "criticality":      r["criticality"],
            "run_config_name":  r["run_config_name"],
            "filter":           r.get("filter"),
            "user_metadata":    r.get("user_metadata"),
            "check": {
                "function":        fn,
                "for_each_column": fec if fec else None,
                "arguments":       args,
            },
        })

    status = DQEngine.validate_checks(raw_rules)
    if getattr(status, "has_errors", False):
        keep: List[dict] = []
        skipped: List[str] = []
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if getattr(st, "has_errors", False):
                skipped.append(r.get("name") or "<unnamed>")
            else:
                keep.append(r)
        return _group_by_table(keep), coerced, len(skipped)
    else:
        return _group_by_table(raw_rules), coerced, 0

# --------------------------
# Apply with isolation and diagnostics
# --------------------------
def _apply_rules_isolating_failures(dq: DQEngine,
                                    src: DataFrame,
                                    table_name: str,
                                    tbl_rules: List[dict]) -> Tuple[Optional[DataFrame], List[Tuple[str, str]]]:
    try:
        return dq.apply_checks_by_metadata(src, tbl_rules), []
    except Exception:
        bad: List[Tuple[str, str]] = []
        good: List[dict] = []
        for r in tbl_rules:
            try:
                dq.apply_checks_by_metadata(src, [r])
                good.append(r)
            except Exception as ex:
                bad.append((r.get("name") or "<unnamed>", str(ex)))
                try:
                    print(f"    offending rule JSON: {json.dumps(r, indent=2)}")
                except Exception:
                    pass

        if bad:
            print(f"[{table_name}] Skipping {len(bad)} bad rule(s):")
            for nm, err in bad:
                print(f"  - {nm}: {err}")
        if not good:
            return None, bad

        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception as ex2:
            print(f"[{table_name}] Still failing after pruning bad rules: {ex2}")
            return None, bad

# --------------------------
# Projection & enrichment
# --------------------------
def _project_row_hits(df_annot: DataFrame,
                      table_name: str,
                      run_config_name: str,
                      created_by: str,
                      exclude_cols: Optional[List[str]] = None) -> DataFrame:
    exclude_cols = set(exclude_cols or [])

    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errors_col   = F.col(e_name) if e_name else _empty_issues_array()
    warnings_col = F.col(w_name) if w_name else _empty_issues_array()

    df = (df_annot
          .withColumn("_errs", errors_col)
          .withColumn("_warns", warnings_col)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]
    row_snapshot = F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value")) for c in sorted(cols)])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_warns")))), 256)

    log_id = F.sha2(F.concat_ws("||",
                                F.lit(table_name),
                                F.lit(run_config_name),
                                row_snapshot_fp,
                                _errors_fp,
                                _warnings_fp), 256)

    return df.select(
        log_id.alias("log_id"),
        F.lit(None).cast(T.ArrayType(T.StringType())).alias("check_id"),  # filled later
        F.lit(table_name).alias("table_name"),
        F.lit(run_config_name).alias("run_config_name"),
        F.col("_errs").alias("_errors"),
        _errors_fp.alias("_errors_fingerprint"),
        F.col("_warns").alias("_warnings"),
        _warnings_fp.alias("_warnings_fingerprint"),
        row_snapshot.alias("row_snapshot"),
        row_snapshot_fp.alias("row_snapshot_fingerprint"),
        F.lit(created_by).alias("created_by"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast(T.StringType()).alias("updated_by"),
        F.lit(None).cast(T.TimestampType()).alias("updated_at"),
    )

def _enrich_check_ids(row_log_df: DataFrame, checks_table: str) -> DataFrame:
    """
    Attach the originating config.check_id(s) to each row_log by joining on:
      lower(table_name), run_config_name, and lower(rule name) from _errors/_warnings.
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Config side: (table, run_config, name) -> check_id
    cfg = (
        spark.table(checks_table)
        .select(
            F.lower(F.col("table_name")).alias("t_tbl_norm"),
            F.col("run_config_name").alias("t_rc"),
            F.lower(F.col("name")).alias("t_name_norm"),
            F.col("check_id").alias("cfg_check_id"),
            F.col("active").alias("t_active")
        )
        .where(F.col("t_active") == True)
        .drop("t_active")
        .dropDuplicates(["t_tbl_norm", "t_rc", "t_name_norm"])
    )

    # Row log side: explode names from _errors/_warnings, normalize & join
    names = (
        row_log_df
        .select(
            "log_id",
            F.lower(F.col("table_name")).alias("tbl_norm"),
            F.col("run_config_name").alias("rc"),
            F.expr("transform(_errors, x -> x.name)").alias("e_names"),
            F.expr("transform(_warnings, x -> x.name)").alias("w_names"),
        )
        .withColumn("all_names", F.array_union("e_names", "w_names"))
        .withColumn("name", F.explode_outer("all_names"))
        .where(F.col("name").isNotNull())
        .withColumn("name_norm", F.lower(F.trim(F.col("name"))))
        .select("log_id","tbl_norm","rc","name_norm")
    )

    j = (
        names.join(
            cfg,
            (names.tbl_norm == cfg.t_tbl_norm) &
            (names.rc == cfg.t_rc) &
            (names.name_norm == cfg.t_name_norm),
            "left"
        )
        .groupBy("log_id")
        .agg(F.array_sort(F.array_distinct(F.collect_list("cfg_check_id"))).alias("check_id"))
    )

    out = (
        row_log_df.drop("check_id")
        .join(j, "log_id", "left")
        .withColumn("check_id", F.coalesce(F.col("check_id"), F.array().cast(T.ArrayType(T.StringType()))))
    )
    return out

# --------------------------
# Summaries
# --------------------------
def _summarize_table(annot: DataFrame, table_name: str) -> Row:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    # Rows with any errors / any warnings (row counts, not sum of array sizes)
    error_rows   = annot.where(F.size(F.col(err)) > 0).count()
    warning_rows = annot.where(F.size(F.col(wrn)) > 0).count()
    total_rows   = annot.count()
    total_flagged_rows = annot.where((F.size(F.col(err)) > 0) | (F.size(F.col(wrn)) > 0)).count()

    # Distinct rule names that fired
    rules_fired = (
        annot.select(
            F.explode_outer(
                F.array_union(
                    F.expr(f"transform({err}, x -> x.name)"),
                    F.expr(f"transform({wrn}, x -> x.name)")
                )
            ).alias("nm")
        )
        .where(F.col("nm").isNotNull())
        .agg(F.countDistinct("nm").alias("rules"))
        .collect()[0]["rules"]
    )

    return Row(table_name=table_name,
               table_total_rows=int(total_rows),
               table_total_error_rows=int(error_rows),
               table_total_warning_rows=int(warning_rows),
               total_flagged_rows=int(total_flagged_rows),
               distinct_rules_fired=int(rules_fired))

def _rules_hits_for_table(annot: DataFrame, table_name: str) -> DataFrame:
    """
    Return per-rule counts (rows flagged) for this table, split by severity.
    """
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    errs = (
        annot
        .select(F.explode_outer(F.expr(f"transform({err}, x -> x.name)")).alias("name"))
        .where(F.col("name").isNotNull())
        .withColumn("severity", F.lit("error"))
    )

    warns = (
        annot
        .select(F.explode_outer(F.expr(f"transform({wrn}, x -> x.name)")).alias("name"))
        .where(F.col("name").isNotNull())
        .withColumn("severity", F.lit("warning"))
    )

    both = errs.unionByName(warns, allowMissingColumns=True)
    return (
        both.groupBy("name", "severity")
        .agg(F.count(F.lit(1)).alias("rows_flagged"))
        .withColumn("table_name", F.lit(table_name))
    )

# ---------------
# Main entry point
# ---------------
def run_checks(
    dqx_cfg_yaml: str,
    created_by: str = "AdminUser",
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive"  # or "strict"
):
    spark = SparkSession.builder.getOrCreate()
    dq = DQEngine(WorkspaceClient())

    cfg = read_yaml(dqx_cfg_yaml)
    checks_table  = cfg["dqx_checks_config_table_name"]
    results_table = cfg["dqx_checks_log_table_name"]
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}

    # Ensure results table exists and apply docs
    ensure_table(results_table, DQX_CHECKS_LOG_METADATA)

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []   # grand rollup across RCs
    printed_grand_once = False

    for rc_name, rc_cfg in rc_map.items():
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        display_section(f"Run config: {rc_name}")

        write_mode = (rc_cfg or {}).get("output_config", {}).get("mode", "overwrite")
        write_opts = (rc_cfg or {}).get("output_config", {}).get("options", {}) or {}

        by_tbl, coerced, skipped = _load_checks_from_table_as_dicts(
            spark, checks_table, rc_name, coercion_mode=coercion_mode
        )
        checks_loaded = sum(len(v) for v in by_tbl.values())
        print(f"[{rc_name}] checks_in_table_total={checks_table_total}, loaded={checks_loaded}, coerced={coerced}, skipped_invalid={skipped}")

        if not checks_loaded:
            print(f"[{rc_name}] no checks loaded (active=TRUE & run_config_name='{rc_name}').")
            continue

        out_batches: List[DataFrame] = []
        rc_tbl_summaries: List[Row] = []
        rc_rule_hit_parts: List[DataFrame] = []
        table_row_counts: Dict[str, int] = {}

        processed_tables = []

        for tbl, tbl_rules in by_tbl.items():
            try:
                src = spark.read.table(tbl)
                annot, bad = _apply_rules_isolating_failures(dq, src, tbl, tbl_rules)
                if annot is None:
                    continue
            except Exception as e:
                print(f"[{rc_name}] {tbl} failed: {e}")
                continue

            processed_tables.append(tbl)

            # Cache / compute total table rows ONCE
            total_rows = annot.count()
            table_row_counts[tbl] = total_rows

            # Per-table summary (row counts & rules fired)
            summary_row = _summarize_table(annot, tbl)
            rc_tbl_summaries.append(summary_row)
            all_tbl_summaries.append(Row(run_config_name=rc_name, **summary_row.asDict()))

            # Per-table, per-rule row-hit counts
            rc_rule_hit_parts.append(_rules_hits_for_table(annot, tbl))

            # Build the row-log payload
            row_hits = _project_row_hits(annot, tbl, rc_name, created_by, exclude_cols=exclude_cols)
            if row_hits.limit(1).count() > 0:
                out_batches.append(row_hits)

        # Show THIS RC’s table summary
        if rc_tbl_summaries:
            summary_df = spark.createDataFrame(rc_tbl_summaries).orderBy("table_name")
            display_section(f"Row-hit summary by table (run_config={rc_name})")
            show_df(summary_df, n=200, truncate=False)

        # Show THIS RC’s rule summary (per-table, includes table_name + % of table rows)
        if rc_rule_hit_parts:
            rules_all = rc_rule_hit_parts[0]
            for part in rc_rule_hit_parts[1:]:
                rules_all = rules_all.unionByName(part, allowMissingColumns=True)

            # Bring in all rules for processed tables (to include zero-hit rules)
            cfg_rules = (
                spark.table(checks_table)
                .where((F.col("run_config_name") == rc_name) & (F.col("active") == True))
                .where(F.col("table_name").isin(processed_tables))
                .select(
                    F.col("table_name"),
                    F.col("name").alias("rule_name"),
                    F.when(F.lower("criticality").isin("warn", "warning"), F.lit("warning"))
                     .otherwise(F.lit("error")).alias("severity")
                )
                .dropDuplicates(["table_name","rule_name","severity"])
            )

            # Prepare counts by (table, rule, severity)
            counts = (
                rules_all
                .groupBy("table_name", "name", "severity")
                .agg(F.sum("rows_flagged").alias("rows_flagged"))
                .withColumnRenamed("name", "rule_name")
            )

            # Left join to include zero-hit rules
            full_rules = (
                cfg_rules.join(counts,
                               on=["table_name","rule_name","severity"],
                               how="left")
                .withColumn("rows_flagged", F.coalesce(F.col("rows_flagged"), F.lit(0)))
            )

            # Attach table total rows & percentage
            totals_df = spark.createDataFrame(
                [Row(table_name=k, table_total_rows=v) for k, v in table_row_counts.items()]
            )
            full_rules = (
                full_rules.join(totals_df, "table_name", "left")
                .withColumn(
                    "pct_of_table_rows",
                    F.when(F.col("table_total_rows") > 0,
                           F.col("rows_flagged") / F.col("table_total_rows"))
                     .otherwise(F.lit(0.0))
                )
                .select("table_name", "rule_name", "severity", "rows_flagged",
                        "table_total_rows", "pct_of_table_rows")
                .orderBy(F.desc("rows_flagged"), F.asc("table_name"), F.asc("rule_name"))
            )

            display_section(f"Row-hit summary by rule (run_config={rc_name})")
            show_df(full_rules, n=2000, truncate=False)

            # Persist per-rule summary for this RC (append)
            full_rules.withColumn("run_config_name", F.lit(rc_name)) \
                .select("run_config_name","table_name","rule_name","severity",
                        "rows_flagged","table_total_rows","pct_of_table_rows") \
                .write.format("delta").mode("append").saveAsTable("dq_dev.dqx.checks_log_summary_by_rule")

        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = out_batches[0]
        for b in out_batches[1:]:
            out = out.unionByName(b, allowMissingColumns=True)

        # Attach config.check_id(s)
        out = _enrich_check_ids(out, checks_table)

        # Persist detailed row log
        out = out.select([f.name for f in ROW_LOG_SCHEMA.fields])
        rows = out.count()
        out.write.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_table}")

    # Grand rollup across RCs — print once
    if all_tbl_summaries and not printed_grand_once:
        grand_df = (
            spark.createDataFrame(all_tbl_summaries)
            .select(
                F.col("run_config_name"),
                F.col("table_name"),
                F.col("table_total_rows"),
                F.col("table_total_error_rows"),
                F.col("table_total_warning_rows"),
                F.col("total_flagged_rows"),
                F.col("distinct_rules_fired"),
            )
            .orderBy("run_config_name", "table_name")
        )
        display_section("Row-hit summary by table (ALL run_configs)")
        show_df(grand_df, n=500, truncate=False)

        # Persist table summary with renamed columns
        grand_df.write.format("delta").mode("overwrite").saveAsTable("dq_dev.dqx.checks_log_summary_by_table")
        printed_grand_once = True

    display_section("Grand total")
    print(f"TOTAL rows written: {grand_total}")


# ---- run it ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"],
    coercion_mode="strict"   # 'strict'| 'permissive'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# === Inline DQX checks (from your wkdy_dim_project.yaml), run them, and keep ONLY errors/warnings columns added ===
from collections import defaultdict
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import functions as F

# ---- 1) Define checks inline (YAML in this cell) ----
checks_yaml = """
- table_name: de_prd.gold.wkdy_dim_project
  name: project_key_is_not_unique
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments:
      columns: [project_key]

- table_name: de_prd.gold.wkdy_dim_project
  name: project_key_is_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    arguments:
      column: project_key

- table_name: de_prd.gold.wkdy_dim_project
  name: project_status_is_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    arguments:
      column: project_status

- table_name: de_prd.gold.wkdy_dim_project
  name: project_status_is_new_value
  criticality: error
  run_config_name: default
  check:
    function: is_in_list
    arguments:
      column: project_status
      allowed:
        - Active
        - Closed
        - Pending Close
        - Schedule Pending
        - Suspended

- table_name: de_prd.gold.wkdy_dim_project
  name: project_type_is_not_null_or_unknown
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments:
      expression: "project_type_name is not null AND project_type_name != 'Unknown'"

- table_name: de_prd.gold.wkdy_dim_project
  name: any_email_has_invalid_format
  criticality: error
  run_config_name: default
  check:
    function: regex_match
    for_each_column:
      - project_leader_email
      - project_biller_email
      - customer_leader_email
      - customer_biller_email
    arguments:
      regex: "^(.+)@(.+)$"

- table_name: de_prd.gold.wkdy_dim_project
  name: project_start_after_end_date
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments:
      expression: "coalesce(project_start_date, '1900-01-01') <= coalesce(project_end_date, '9999-12-31')"

- table_name: de_prd.gold.wkdy_dim_project
  name: first_activity_date_after_last_activity_date
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments:
      expression: "coalesce(first_activity_date, '1900-01-01') <= coalesce(last_activity_date, '9999-12-31')"

- table_name: de_prd.gold.wkdy_dim_project
  name: created_date_after_last_updated_date
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments:
      expression: "coalesce(_created_date, '1900-01-01') <= coalesce(_last_updated_date, '9999-12-31')"
"""

checks = yaml.safe_load(checks_yaml)
if isinstance(checks, dict):
    checks = [checks]

# Group rules by table
by_table = defaultdict(list)
for r in checks:
    t = r.get("table_name")
    if t:
        by_table[t].append(r)

print(f"Loaded {sum(len(v) for v in by_table.values())} rule(s) "
      f"across {len(by_table)} table(s): {list(by_table.keys())}")

# ---- 2) Validate & apply ----
dq = DQEngine(WorkspaceClient())

# Validate once (will raise if bad)
status = dq.validate_checks(checks)
if getattr(status, "has_errors", False):
    # if you prefer non-fatal, comment the raise and print status.to_string()
    raise ValueError(status.to_string())

for table, tbl_rules in by_table.items():
    print(f"\n=== Applying {len(tbl_rules)} rule(s) to {table} ===")
    df = spark.table(table)

    annotated = dq.apply_checks_by_metadata(df, tbl_rules)

    # DQX may use _error/_warning or _errors/_warnings depending on version
    err_col = "_errors"  if "_errors"  in annotated.columns else "_error"
    wrn_col = "_warnings" if "_warnings" in annotated.columns else "_warning"

    # Add normalized columns named 'errors' and 'warnings' (keep originals too)
    annotated = (annotated
                 .withColumn("errors",   F.col(err_col))
                 .withColumn("warnings", F.col(wrn_col)))

    # Simple summary (no RDD usage)
    agg = (
        annotated
        .select(
            F.size(F.col(err_col)).alias("e_sz"),
            F.size(F.col(wrn_col)).alias("w_sz"),
        )
        .agg(
            F.coalesce(F.sum("e_sz"), F.lit(0)).alias("errors"),
            F.coalesce(F.sum("w_sz"), F.lit(0)).alias("warnings"),
        )
        .collect()[0]
    )
    print(f"Hit summary -> errors={int(agg['errors'])}, warnings={int(agg['warnings'])}")

    # Show only rows that have any hits, with the new columns
    hits_only = annotated.filter(
        (F.col(err_col).isNotNull() & (F.size(F.col(err_col)) > 0)) |
        (F.col(wrn_col).isNotNull() & (F.size(F.col(wrn_col)) > 0))
    )

    try:
        display(hits_only.select("*"))   # notebook
    except NameError:
        hits_only.select("*").show(20, truncate=False)

    # OPTIONAL: persist annotated rows (with errors/warnings) to a Delta table
    # Uncomment and set a destination if you want to save:
    # results_table = "dq_dev.dqx.wkdy_dim_project_results"
    # (hits_only
    #   .select("*")   # includes 'errors' and 'warnings'
    #   .write.format("delta").mode("overwrite").saveAsTable(results_table))
    # print(f"Wrote {results_table}")
