# Databricks notebook source
# MAGIC %md
# MAGIC # Run DQX Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference

# COMMAND ----------

# DBTITLE 1,FLOW
"""
START: run_checks(dqx_cfg_yaml, created_by, time_zone, exclude_cols, coercion_mode)
|
|-- 0. Env banner
|     |-- print_notebook_env(spark, local_timezone=time_zone)
|
|-- 1. Load config YAML
|     |-- cfg = read_yaml(dqx_cfg_yaml)
|     |-- checks_table  = cfg["dqx_checks_config_table_name"]
|     |-- results_table = cfg["dqx_checks_log_table_name"]
|     |-- rc_map        = cfg.get("run_config_name", {})
|
|-- 2. Ensure results table exists (Delta + comments)
|     |-- ensure_table(results_table, DQX_CHECKS_LOG_METADATA)
|
|-- 3. (Optional) Count active checks for banner
|     |-- checks_table_total = COUNT(*) FROM checks_table WHERE active = TRUE
|
|-- 4. Init grand trackers
|     |-- grand_total = 0
|     |-- all_tbl_summaries = []
|
|-- 5. For each run_config_name in rc_map:
|     |-- DISPLAY section "Run config: {rc_name}"
|     |-- read write_mode / write_opts from rc_cfg.output_config (default overwrite)
|
|     |-- 5.1 Load checks for this RC from config table
|     |     |-- Filter: active=TRUE AND run_config_name='{rc_name}'
|     |     |-- Select: table_name, name, criticality, filter, run_config_name, user_metadata, check, check_id, check_id_payload
|     |     |-- JIT argument coercion (per rule) using _coerce_arguments(function=check.function, mode=coercion_mode)
|     |     |-- Build minimal exec rules for DQEngine
|     |     |-- Validate with DQEngine.validate_checks:
|     |           |-- If batch has errors: keep only per-rule valid ones; skipped_invalid = count(bad)
|     |     |-- by_tbl = rules grouped by table_name
|     |     |-- cfg_id_df = mapping DF[t_tbl_norm, t_rc, t_name_norm, cfg_check_id] (computed deterministically;
|     |                     derives check_id if missing via _canonical_rule_payload_and_id)
|     |     |-- PRINT: checks_in_table_total, loaded, coerced, skipped_invalid
|
|     |-- 5.2 Apply rules per table with isolation
|     |     |-- For each table in by_tbl:
|     |           |-- src = spark.read.table(table)
|     |           |-- (annot, bad_rules) = _apply_rules_isolating_failures(DQEngine, src, rules_for_table)
|     |           |     |-- If batch fails: try each rule; drop offenders; retry with good subset
|     |           |-- If annot is None: continue
|     |           |-- total_rows = annot.count()
|     |           |-- summary_row = _summarize_table(annot, table)
|     |           |-- rc_tbl_summaries += summary_row
|     |           |-- all_tbl_summaries += Row(run_config_name=rc_name, **summary_row)
|     |           |-- rc_rule_hit_parts += _rules_hits_for_table(annot, table)
|     |           |-- row_hits_df = _project_row_hits(
|     |           |       annot, table, rc_name, created_by, exclude_cols
|     |           |   )
|     |           |   |-- Computes:
|     |           |   |     row_snapshot = [{column, value}] over non-reserved cols (stringified)
|     |           |   |     row_snapshot_fingerprint = sha256(JSON(row_snapshot))
|     |           |   |     _errors/_warnings normalized & fingerprinted (stable across order/column changes)
|     |           |   |     log_id = sha256(table_name || run_config_name || row_snapshot_fp || _errors_fp || _warnings_fp)
|     |           |-- If row_hits_df has rows: out_batches += row_hits_df
|
|     |-- 5.3 Display per-RC summary by table (if any)
|     |     |-- DISPLAY rc_tbl_summaries ordered by table_name
|
|     |-- 5.4 Build per-RC summary by rule (includes zero-hit rules)
|     |     |-- rules_all = UNION of rc_rule_hit_parts
|     |     |-- cfg_rules = DISTINCT rules from checks_table for this RC limited to processed tables
|     |     |-- counts = rules_all GROUP BY (table_name, name, severity) SUM(rows_flagged)
|     |     |-- full_rules = cfg_rules LEFT JOIN counts; NULL→0
|     |     |-- totals_df = table_row_counts → compute pct_of_table_rows
|     |     |-- DISPLAY full_rules
|     |     |-- APPEND full_rules (+ run_config_name) → dq_dev.dqx.checks_log_summary_by_rule
|
|     |-- 5.5 If there are row-level hits:
|     |     |-- out = UNION of out_batches
|     |     |-- Enrich check_id(s):
|     |     |     |-- out = _enrich_check_ids(out, cfg_id_df=cfg_id_df, checks_table=checks_table)
|     |     |     |   |-- Join on (run_config_name, normalized rule name) and flexible table keys
|     |     |-- Project to ROW_LOG_SCHEMA column order
|     |     |-- rows = out.count()
|     |     |-- WRITE out.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
|     |     |-- grand_total += rows
|     |     |-- PRINT "[rc_name] wrote {rows} rows → {results_table}"
|     |-- else:
|     |     |-- PRINT "[rc_name] no row-level hits."
|
|-- 6. Grand rollup across all RCs (if any summaries)
|     |-- grand_df = all_tbl_summaries (run_config_name, table_name, table_total_rows,
|     |                                 table_total_error_rows, table_total_warning_rows,
|     |                                 total_flagged_rows, distinct_rules_fired)
|     |-- DISPLAY grand_df
|     |-- OVERWRITE grand_df → dq_dev.dqx.checks_log_summary_by_table
|
|-- 7. Final banner
|     |-- DISPLAY section "Grand total"
|     |-- PRINT "TOTAL rows written: {grand_total}"
|
END: run_checks
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementation

# COMMAND ----------

# DBTITLE 1,Install DQX Package
# MAGIC %pip install databricks-labs-dqx==0.8.0

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Run DQX Checks & Log Results
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
from utils.print import print_notebook_env
from utils.timezone import current_time_iso

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
    print("\n" + f"{Color.b}{Color.deep_magenta}═{Color.r}" *80)
    print(f"{Color.b}{Color.deep_magenta}║{Color.r} {Color.b}{Color.ghost_white}{title}{Color.r}")
    print(f"{Color.b}{Color.deep_magenta}═{Color.r}"* 80)

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
    "table": "dq_dev.dqx.checks_log",
    "table_comment": (
        "## DQX Row-Level Check Results Log\n"
        "- One row per **flagged source row** (error or warning) emitted by DQX for a given run.\n"
        "- **Primary key**: `log_id` = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp). "
        "**No duplicates allowed** within a run; duplicates indicate a repeated write or misconfigured mode.\n"
        "- `_errors/_warnings` capture issue structs; corresponding fingerprints are deterministic and order-insensitive.\n"
        "- `row_snapshot` captures the offending row’s non-reserved columns (values stringified) at evaluation time.\n"
        "- `check_id` lists originating rule IDs from the config table; may be empty if name-based mapping was not possible.\n"
        "- Writers should ensure idempotency (e.g., overwrite mode or dedupe by `log_id` when appending)."
    ),
    "columns": {
        "log_id": "PRIMARY KEY. Deterministic sha256 over (table_name, run_config_name, row_snapshot_fingerprint, _errors_fingerprint, _warnings_fingerprint).",
        "check_id": "Array of originating config `check_id`s that fired for this row (may be empty if unmapped).",
        "table_name": "Source table FQN (`catalog.schema.table`) where the row was evaluated.",
        "run_config_name": "Run configuration under which the checks executed.",
        "_errors": "Array<struct> of error issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_errors_fingerprint": "Deterministic digest of normalized `_errors` (order/column-order insensitive).",
        "_warnings": "Array<struct> of warning issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_warnings_fingerprint": "Deterministic digest of normalized `_warnings`.",
        "row_snapshot": "Array<struct{column:string, value:string}> of non-reserved columns for the flagged row (stringified).",
        "row_snapshot_fingerprint": "sha256(JSON(row_snapshot)) used in `log_id` and de-duplication.",
        "created_by": "Audit: writer identity for this record.",
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
    escaped =_esc_sql_comment(comment)
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
        cat, sch,_ = full_name.split(".")
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
    # FIX: align with DQX signature for sql_query (no 'limit')
    "sql_query": {
        "query": "str",
        "merge_columns": "list",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "condition_column": "str",
        "input_placeholder": "str",
        "row_filter": "str",
    },
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
    raw = {k:_parse_scalar(v) for k, v in args_map.items()}
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
            # Pass through unknown keys untouched so DQX can validate them
            out[k] = v

    # REMOVED: hard requirement for 'limit' on sql_query (not part of DQX signature)
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
    Attach originating config.check_id(s) to each row_log by joining on:
      - run_config_name
      - normalized rule name (lowercased, trimmed)
      - a flexible table key that can be:
          catalog.schema.table  OR  schema.table  OR  table
    This handles cases where the config used 1- or 2-part names while the row log
    carries a fully-qualified 3-part FQN.
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # --- Config side: (table variants, run_config, rule name) -> check_id
    cfg_base = (
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
        .dropDuplicates(["t_tbl_norm", "t_rc", "t_name_norm", "cfg_check_id"])
    )

    cfg_with_keys = (
        cfg_base
        .withColumn("arr", F.array(
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 3,
                   F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -3),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -2),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 2,
                   F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -2),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.element_at(F.split("t_tbl_norm", r"\."), -1)
        ))
        .withColumn("cfg_tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
        .drop("arr")
    )

    # --- Row-log side: explode names from _errors/_warnings, normalize & make table variants
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

    names_with_keys = (
        names
        .withColumn("arr", F.array(
            F.when(F.size(F.split("tbl_norm", r"\.")) >= 3,
                   F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -3),
                                     F.element_at(F.split("tbl_norm", r"\."), -2),
                                     F.element_at(F.split("tbl_norm", r"\."), -1))),
            F.when(F.size(F.split("tbl_norm", r"\.")) >= 2,
                   F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -2),
                                     F.element_at(F.split("tbl_norm", r"\."), -1))),
            F.element_at(F.split("tbl_norm", r"\."), -1)
        ))
        .withColumn("tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
        .drop("arr")
    )

    # --- Join on RC + rule name + any compatible table key
    matched = (
        names_with_keys
        .join(
            cfg_with_keys,
            (names_with_keys.rc == cfg_with_keys.t_rc) &
            (names_with_keys.name_norm == cfg_with_keys.t_name_norm) &
            (names_with_keys.tbl_key == cfg_with_keys.cfg_tbl_key),
            "left"
        )
        .where(F.col("cfg_check_id").isNotNull())
        .groupBy("log_id")
        .agg(
            F.array_sort(
                F.array_distinct(
                    F.collect_list(F.col("cfg_check_id").cast(T.StringType()))
                )
            ).alias("check_id")
        )
    )

    # --- Attach back to the row-log; default to empty array if no match
    out = (
        row_log_df.drop("check_id")
        .join(matched, "log_id", "left")
        .withColumn("check_id",
                    F.coalesce(F.col("check_id"),
                               F.array().cast(T.ArrayType(T.StringType()))))
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
    time_zone: str = "America/Chicago",
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive"  # or "strict"
):
    spark = SparkSession.builder.getOrCreate()
    dq = DQEngine(WorkspaceClient())

    print_notebook_env(spark, local_timezone=time_zone)

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
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

# ---- run it ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"],
    coercion_mode="strict"   # 'strict'| 'permissive'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing

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
from utils.print import print_notebook_env

# -------------------
# Display helpers
# -------------------
def _can_display() -> bool:
    return "display" in globals()

def show_df(df: DataFrame, n: int = 100, truncate: bool = False) -> None:
    if _can_display():
        display(df.limit(n))
    else:
        df.show(n, truncate=truncate)

def display_section(title: str) -> None:
    print("\n" + f"{Color.b}{Color.deep_magenta}═{Color.r}" *80)
    print(f"{Color.b}{Color.deep_magenta}║{Color.r} {Color.b}{Color.ghost_white}{title}{Color.r}")
    print(f"{Color.b}{Color.deep_magenta}═{Color.r}"* 80)

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
    # (no top-level check_id)
    T.StructField("table_name",                  T.StringType(),  False),
    T.StructField("run_config_name",             T.StringType(),  False),

    # _errors (array of issue structs; includes check_id inside each element)
    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False),
    T.StructField("_errors_fingerprint",         T.StringType(),  False),

    # _warnings (array of issue structs; includes check_id inside each element)
    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False),
    T.StructField("_warnings_fingerprint",       T.StringType(),  False),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",        T.StringType(), False),
        T.StructField("value",         T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint",    T.StringType(),  False),

    T.StructField("created_by",                  T.StringType(),  False),
    T.StructField("created_at",                  T.TimestampType(), False),
    T.StructField("updated_by",                  T.StringType(),  True),
    T.StructField("updated_at",                  T.TimestampType(), True),
])

# -------------------
# Documentation dictionary
# -------------------
DQX_CHECKS_LOG_METADATA: Dict[str, Any] = {
    "table": "dq_dev.dqx.checks_log",
    "table_comment": (
        "## DQX Row-Level Check Results Log\n"
        "- One row per **flagged source row** (error or warning) emitted by DQX for a given run.\n"
        "- **Primary key**: `log_id` = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp). "
        "**No duplicates allowed** within a run; duplicates indicate a repeated write or misconfigured mode.\n"
        "- `_errors/_warnings` capture issue structs; corresponding fingerprints are deterministic and order-insensitive.\n"
        "- `row_snapshot` captures the offending row’s non-reserved columns (values stringified) at evaluation time.\n"
        "- `check_id` lists originating rule IDs from the config table; may be empty if name-based mapping was not possible.\n"
        "- Writers should ensure idempotency (e.g., overwrite mode or dedupe by `log_id` when appending)."
    ),
    "columns": {
        "log_id": "PRIMARY KEY. Deterministic sha256 over (table_name, run_config_name, row_snapshot_fingerprint, _errors_fingerprint, _warnings_fingerprint).",
        "check_id": "Array of originating config `check_id`s that fired for this row (may be empty if unmapped).",
        "table_name": "Source table FQN (`catalog.schema.table`) where the row was evaluated.",
        "run_config_name": "Run configuration under which the checks executed.",
        "_errors": "Array<struct> of error issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_errors_fingerprint": "Deterministic digest of normalized `_errors` (order/column-order insensitive).",
        "_warnings": "Array<struct> of warning issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_warnings_fingerprint": "Deterministic digest of normalized `_warnings`.",
        "row_snapshot": "Array<struct{column:string, value:string}> of non-reserved columns for the flagged row (stringified).",
        "row_snapshot_fingerprint": "sha256(JSON(row_snapshot)) used in `log_id` and de-duplication.",
        "created_by": "Audit: writer identity for this record.",
        "created_at": "Audit: creation timestamp (UTC).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (UTC, nullable).",
    },
}

# -------------------
# Comment application helpers
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
    cat, sch, tbl = fqn.split(".")
    escaped = _esc_sql_comment(comment)
    try:
        spark.sql(
            f"COMMENT ON COLUMN `{cat}`.`{sch}`.`{tbl}`.`{col_name}` IS '{escaped}'"
        )
        return
    except Exception:
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
        cat, sch,_ = full_name.split(".")
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
    # inline the exact issue struct type used in schema
    issue_struct_type = T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])
    return F.from_json(F.lit("[]"), T.ArrayType(issue_struct_type))

def _normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # stable fingerprint: sort 'columns'; drop run_time/user_metadata/check_id
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
# JIT argument coercion (unchanged)
# --------------------------
_EXPECTED: Dict[str, Dict[str, str]] = {
    "is_unique": {"columns": "list"},
    "is_in_list": {"column": "str", "allowed": "list"},
    "is_in_range": {"column": "str", "min_limit": "num", "max_limit": "num",
                    "inclusive_min": "bool", "inclusive_max": "bool"},
    "regex_match": {"column": "str", "regex": "str"},
    "sql_expression": {"expression": "str"},
    "sql_query": {
        "query": "str",
        "merge_columns": "list",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "condition_column": "str",
        "input_placeholder": "str",
        "row_filter": "str",
    },
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
    raw = {k:_parse_scalar(v) for k, v in args_map.items()}
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

    if mode == "strict" and errs:
        raise ValueError(f"Argument coercion failed for '{function_name}': {errs}")
    return out, errs

# --------------------------
# Load rules from the table
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
# Projection (build row_log skeleton)
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

# --------------------------
# Embed check_id into each issue element (no top-level check_id)
# --------------------------
def _embed_issue_check_ids(row_log_df: DataFrame, checks_table: str) -> DataFrame:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Config keys (INCLUDES FILTER) + flexible table keys
    cfg_base = (
        spark.table(checks_table)
        .where(F.col("active") == True)
        .select(
            F.lower(F.col("table_name")).alias("t_tbl_norm"),
            F.col("run_config_name").alias("t_rc"),
            F.lower(F.trim(F.col("name"))).alias("t_name_norm"),
            F.lower(F.trim(F.coalesce(F.col("filter"), F.lit("")))).alias("t_filter_norm"),
            F.col("check_id").alias("cfg_check_id")
        )
        .dropDuplicates(["t_tbl_norm","t_rc","t_name_norm","t_filter_norm","cfg_check_id"])
    )

    cfg_keys = (
        cfg_base
        .withColumn("arr", F.array(
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 3,
                   F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -3),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -2),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 2,
                   F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -2),
                                     F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.element_at(F.split("t_tbl_norm", r"\."), -1)
        ))
        .withColumn("cfg_tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
        .drop("arr")
        .dropDuplicates(["cfg_tbl_key","t_rc","t_name_norm","t_filter_norm","cfg_check_id"])
    )

    # Exact issue struct type we want in the arrays
    issue_struct_type = T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])

    def enrich(colname: str) -> DataFrame:
        # explode with position
        log_side = (
            row_log_df
            .select("log_id", "table_name", "run_config_name",
                    F.posexplode_outer(F.col(colname)).alias("pos", "iss"))
            .withColumn("tbl_norm", F.lower(F.col("table_name")))
            .withColumn("rc", F.col("run_config_name"))
            .withColumn("name_norm", F.lower(F.trim(F.col("iss.name"))))
            .withColumn("filter_norm", F.lower(F.trim(F.coalesce(F.col("iss.filter"), F.lit("")))))
            .withColumn("arr", F.array(
                F.when(F.size(F.split("tbl_norm", r"\.")) >= 3,
                       F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -3),
                                         F.element_at(F.split("tbl_norm", r"\."), -2),
                                         F.element_at(F.split("tbl_norm", r"\."), -1))),
                F.when(F.size(F.split("tbl_norm", r"\.")) >= 2,
                       F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -2),
                                         F.element_at(F.split("tbl_norm", r"\."), -1))),
                F.element_at(F.split("tbl_norm", r"\."), -1)
            ))
            .withColumn("tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
            .drop("arr")
        )

        matched = (
            log_side
            .join(
                cfg_keys,
                (F.col("rc") == F.col("t_rc")) &
                (F.col("name_norm") == F.col("t_name_norm")) &
                (F.col("filter_norm") == F.col("t_filter_norm")) &
                (F.col("tbl_key") == F.col("cfg_tbl_key")),
                "left"
            )
            .groupBy("log_id", "pos")
            .agg(
                F.first("iss", ignorenulls=True).alias("iss"),
                F.max("cfg_check_id").alias("issue_check_id")
            )
        )

        # Build the final issue struct as JSON so we can sort on an orderable type
        matched_json = matched.select(
            "log_id",
            "pos",
            F.to_json(F.struct(
                F.col("iss.name").alias("name"),
                F.col("iss.message").alias("message"),
                F.col("iss.columns").alias("columns"),
                F.col("iss.filter").alias("filter"),
                F.col("iss.function").alias("function"),
                F.col("iss.run_time").alias("run_time"),
                F.col("iss.user_metadata").alias("user_metadata"),
                F.col("issue_check_id").cast(T.StringType()).alias("check_id")
            )).alias("iss_json")
        )

        # Reassemble: sort by (pos, iss_json), then parse JSON back to the struct
        return (
            matched_json
            .groupBy("log_id")
            .agg(F.array_sort(F.collect_list(F.struct(F.col("pos"), F.col("iss_json")))).alias("kv"))
            .withColumn(colname, F.transform(F.col("kv"), lambda x: F.from_json(x["iss_json"], issue_struct_type)))
            .select("log_id", colname)
        )

    err_arr = enrich("_errors")
    warn_arr = enrich("_warnings")

    return (
        row_log_df.drop("_errors", "_warnings")
        .join(err_arr, "log_id", "left")
        .join(warn_arr, "log_id", "left")
        .withColumn("_errors", F.coalesce(F.col("_errors"), F.from_json(F.lit("[]"), T.ArrayType(issue_struct_type))))
        .withColumn("_warnings", F.coalesce(F.col("_warnings"), F.from_json(F.lit("[]"), T.ArrayType(issue_struct_type))))
    )

# --------------------------
# Summaries (unchanged)
# --------------------------
def _summarize_table(annot: DataFrame, table_name: str) -> Row:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    error_rows   = annot.where(F.size(F.col(err)) > 0).count()
    warning_rows = annot.where(F.size(F.col(wrn)) > 0).count()
    total_rows   = annot.count()
    total_flagged_rows = annot.where((F.size(F.col(err)) > 0) | (F.size(F.col(wrn)) > 0)).count()

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
# Main entry
# ---------------
def run_checks(
    dqx_cfg_yaml: str,
    created_by: str = "AdminUser",
    time_zone: str = "America/Chicago",
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive"
):
    spark = SparkSession.builder.getOrCreate()
    # allow nested struct evolution (adds 'check_id' inside issue structs if table exists)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dq = DQEngine(WorkspaceClient())
    print_notebook_env(spark, local_timezone=time_zone)

    cfg = read_yaml(dqx_cfg_yaml)
    checks_table  = cfg["dqx_checks_config_table_name"]
    results_table = cfg["dqx_checks_log_table_name"]
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}

    ensure_table(results_table, DQX_CHECKS_LOG_METADATA)

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []
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

            total_rows = annot.count()
            table_row_counts[tbl] = total_rows

            summary_row = _summarize_table(annot, tbl)
            rc_tbl_summaries.append(summary_row)
            all_tbl_summaries.append(Row(run_config_name=rc_name, **summary_row.asDict()))
            rc_rule_hit_parts.append(_rules_hits_for_table(annot, tbl))

            row_hits = _project_row_hits(annot, tbl, rc_name, created_by, exclude_cols=exclude_cols)
            if row_hits.limit(1).count() > 0:
                out_batches.append(row_hits)

        if rc_tbl_summaries:
            summary_df = spark.createDataFrame(rc_tbl_summaries).orderBy("table_name")
            display_section(f"Row-hit summary by table (run_config={rc_name})")
            show_df(summary_df, n=200, truncate=False)

        if rc_rule_hit_parts:
            rules_all = rc_rule_hit_parts[0]
            for part in rc_rule_hit_parts[1:]:
                rules_all = rules_all.unionByName(part, allowMissingColumns=True)

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

            counts = (
                rules_all
                .groupBy("table_name", "name", "severity")
                .agg(F.sum("rows_flagged").alias("rows_flagged"))
                .withColumnRenamed("name", "rule_name")
            )

            full_rules = (
                cfg_rules.join(counts,
                               on=["table_name","rule_name","severity"],
                               how="left")
                .withColumn("rows_flagged", F.coalesce(F.col("rows_flagged"), F.lit(0)))
            )

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

        # Embed check_id inside each issue (errors/warnings)
        out = _embed_issue_check_ids(out, checks_table)

        # Persist detailed row log
        out = out.select([f.name for f in ROW_LOG_SCHEMA.fields])
        rows = out.count()
        out.write.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_table}")

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

        grand_df.write.format("delta").mode("overwrite").saveAsTable("dq_dev.dqx.checks_log_summary_by_table")

        printed_grand_once = True

    display_section("Grand total")
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

# ---- run it ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"],
    coercion_mode="strict"
)
