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

# DBTITLE 1,Target Table Structured Schema
"""
CHECKS_LOG_SCHEMA = T.StructType([
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
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Runbook — 02_run_dqx_checks (Run DQX Checks)
# MAGIC
# MAGIC **Purpose**  
# MAGIC Execute Databricks Labs DQX rules from the config table, write a row‑level results log, and produce lightweight rollups for dashboards.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## At a glance
# MAGIC - **Notebook:** `02_run_dqx_checks`
# MAGIC - **Engine:** `databricks-labs-dqx==0.8.x`
# MAGIC - **Reads:** rules from `dq_{env}.dqx.checks_log` (Delta)
# MAGIC - **Writes:**
# MAGIC   - **Row log:** `dqx_checks_log_table_name` (Delta, one row per flagged source row)
# MAGIC   - **Summary (by rule):** `dq_dev.dqx.checks_log_summary_by_rule` (append)
# MAGIC   - **Summary (by table):** `dq_dev.dqx.checks_log_summary_by_table` (overwrite)
# MAGIC - **Key behavior:** embeds `check_id` **inside each issue element** of `_errors`/`_warnings` (no top‑level `check_id` column). Deterministic `log_id` prevents accidental duplicates within a run.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog schemas exist and you have permission to **read source tables** and **write** to the targets above.
# MAGIC - Rules have been loaded by **01_load_dqx_checks** into the checks config table.
# MAGIC - Cluster/SQL warehouse with Delta support. The notebook enables `spark.databricks.delta.schema.autoMerge.enabled=true` for nested struct evolution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Inputs & Config
# MAGIC YAML at `resources/dqx_config.yaml` (or your path) must define:
# MAGIC ```yaml
# MAGIC dqx_checks_config_table_name: dq_dev.dqx.checks           # rules table (from loader)
# MAGIC dqx_checks_log_table_name:    dq_dev.dqx.checks_log       # results (row log)
# MAGIC run_config_name:                                          # named run configs (RCs)
# MAGIC   nightly:
# MAGIC     output_config:
# MAGIC       mode: overwrite                                     # overwrite|append
# MAGIC       options: {}                                         # any DataFrameWriter options
# MAGIC   ad_hoc:
# MAGIC     output_config:
# MAGIC       mode: append
# MAGIC       options: {}
# MAGIC ```
# MAGIC
# MAGIC **Rules selection per RC:** active rules where `run_config_name == <RC>` are loaded from the checks table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to run
# MAGIC Minimal usage in a cell:
# MAGIC ```python
# MAGIC # One‑time per cluster session
# MAGIC # %pip install databricks-labs-dqx==0.8.0
# MAGIC # dbutils.library.restartPython()
# MAGIC
# MAGIC run_checks(
# MAGIC     dqx_cfg_yaml="resources/dqx_config.yaml",
# MAGIC     created_by="AdminUser",
# MAGIC     time_zone="America/Chicago",
# MAGIC     exclude_cols=None,            # optional: columns to exclude from row_snapshot
# MAGIC     coercion_mode="strict"        # "permissive" or "strict" argument coercion
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Parameters**
# MAGIC - `dqx_cfg_yaml` – path to the YAML above.
# MAGIC - `created_by` – audit field written to the row log.
# MAGIC - `time_zone` – for banners/printing only.
# MAGIC - `exclude_cols` – columns to omit from `row_snapshot` (e.g., large blobs).
# MAGIC - `coercion_mode` – how strictly to coerce stringified rule arguments (`strict` recommended).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What it does (flow)
# MAGIC 1. **Environment banner** and YAML load.  
# MAGIC 2. **Ensure row‑log table exists** (creates empty Delta table with schema & comments if missing).  
# MAGIC 3. For each **run config (RC)** in YAML:  
# MAGIC    a. **Load active rules** for that RC from the checks table; JIT‑coerce argument types; validate with `DQEngine`. Invalid rules are skipped (isolated).  
# MAGIC    b. **Group by table**, read each source table, and call `DQEngine.apply_checks_by_metadata`. If a batch fails, the runner tests rules individually and drops offenders.  
# MAGIC    c. **Project row hits** to the row‑log schema, computing:
# MAGIC       - `row_snapshot` = `[{"column","value"}...]` over non‑reserved columns (stringified)  
# MAGIC       - Fingerprints for `_errors`, `_warnings`, and `row_snapshot`  
# MAGIC       - Deterministic `log_id = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp)`  
# MAGIC    d. **Embed `check_id`** into each issue element (errors & warnings) by matching `(table, run_config_name, rule name, filter)`.  
# MAGIC    e. **Write row log** to `dqx_checks_log_table_name` using RC’s `output_config` (default `overwrite`).  
# MAGIC    f. **Summaries:** display per‑table rollups; build **by‑rule** counts (including zero‑hit rules) and append to `dq_dev.dqx.checks_log_summary_by_rule`.  
# MAGIC 4. **Grand rollup (all RCs)** is written to `dq_dev.dqx.checks_log_summary_by_table` (overwrite).  
# MAGIC 5. **Final banner** prints total rows written.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Row‑Log schema (essential fields)
# MAGIC - `log_id` *(PK)* – deterministic hash; ensures idempotency within a run.  
# MAGIC - `table_name`, `run_config_name` – lineage.  
# MAGIC - `_errors`, `_warnings` – arrays of issue structs:  
# MAGIC   `{ name, message, columns[], filter, function, run_time, user_metadata, check_id }`  
# MAGIC - `_errors_fingerprint`, `_warnings_fingerprint` – order/column‑order insensitive digests.  
# MAGIC - `row_snapshot` – array of `{column, value}` (stringified).  
# MAGIC - `row_snapshot_fingerprint` – digest included in `log_id`.  
# MAGIC - `created_by`, `created_at`, `updated_by`, `updated_at` – audit.
# MAGIC
# MAGIC > **Note:** There is **no** top‑level `check_id` column; IDs live **inside** each issue element.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Operational tips
# MAGIC - **Write mode per RC**: control idempotency via YAML (`overwrite` recommended for scheduled runs; `append` for incremental scenarios).  
# MAGIC - **Schema evolution**: if upgrading from a version without `issue.check_id`, the runner sets `autoMerge=true` to evolve arrays of structs in place.  
# MAGIC - **Performance**: large tables can be slow—filter with rule‑level `filter` predicates in the checks table; consider running by table RCs.  
# MAGIC - **Dashboards**: the downstream views in your dashboard should explode `_errors`/`_warnings` and join by embedded `check_id` when needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC - **“no checks loaded”** for an RC → ensure checks table has `active=true` and exact `run_config_name` match.  
# MAGIC - **Validation failures** → the runner will isolate bad rules and continue; check notebook output for offending rules (JSON logged). Fix the rule in YAML and re‑load with the loader.  
# MAGIC - **Duplicate rows in append mode** → `log_id` is deterministic for the same row snapshot & issues; if inputs change (e.g., timestamps), duplicates are expected. Prefer `overwrite` for repeatable runs.  
# MAGIC - **Missing `check_id` in issues** → verify rule names/filters/table names in checks table match the issues; embedding derives IDs by `(table_key, run_config_name, rule name, filter)`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Verification queries
# MAGIC ```sql
# MAGIC -- Row log rows by run config
# MAGIC SELECT run_config_name, COUNT(*) AS rows
# MAGIC FROM dq_dev.dqx.checks_log
# MAGIC GROUP BY run_config_name
# MAGIC ORDER BY rows DESC;
# MAGIC
# MAGIC -- Any issue elements missing check_id?
# MAGIC SELECT SUM(size(filter(_errors,   x -> x.check_id IS NULL))) AS errors_missing_id,
# MAGIC        SUM(size(filter(_warnings, x -> x.check_id IS NULL))) AS warnings_missing_id
# MAGIC FROM dq_dev.dqx.checks_log;
# MAGIC
# MAGIC -- Latest summaries
# MAGIC SELECT * FROM dq_dev.dqx.checks_log_summary_by_rule   ORDER BY run_config_name, rows_flagged DESC;
# MAGIC SELECT * FROM dq_dev.dqx.checks_log_summary_by_table  ORDER BY run_config_name, table_name;
# MAGIC ```

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

# DBTITLE 1,FINAL
# Databricks notebook: 02_run_dqx_checks
# Purpose: Run DQX checks and write row-level logs + summaries
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

from resources.dqx_functions_0_8_0 import EXPECTED as EXPECTED_ARGS

from utils.color import Color
from utils.runtime import show_notebook_env
from utils.display import show_df, display_section
from utils.config import ProjectConfig
from utils.write import TableWriter
from utils.table import table_exists

# =======================================================================
# Schemas
# =======================================================================
CHECKS_LOG_STRUCT = T.StructType([
    T.StructField("log_id",                  T.StringType(),   False, {"comment": "PRIMARY KEY. sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp)"}),
    T.StructField("table_name",              T.StringType(),   False, {"comment": "Source table (catalog.schema.table)"}),
    T.StructField("run_config_name",         T.StringType(),   False, {"comment": "Run configuration label"}),

    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False, {"comment": "Error issues for this row"}),
    T.StructField("_errors_fingerprint",     T.StringType(),   False, {"comment": "Digest of normalized _errors"}),

    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False, {"comment": "Warning issues for this row"}),
    T.StructField("_warnings_fingerprint",   T.StringType(),   False, {"comment": "Digest of normalized _warnings"}),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",        T.StringType(), False),
        T.StructField("value",         T.StringType(), True),
    ])), False, {"comment": "Stringified non-reserved columns from the source row"}),
    T.StructField("row_snapshot_fingerprint",T.StringType(),   False, {"comment": "Digest of row_snapshot"}),

    T.StructField("created_by",              T.StringType(),   False, {"comment": "Writer identity"}),
    T.StructField("created_at",              T.TimestampType(),False, {"comment": "UTC write time"}),
    T.StructField("updated_by",              T.StringType(),   True,  {"comment": "Last updater (nullable)"}),
    T.StructField("updated_at",              T.TimestampType(),True,  {"comment": "UTC update time (nullable)"}),
])

CHECKS_LOG_SUMMARY_BY_RULE_STRUCT = T.StructType([
    T.StructField("run_config_name",   T.StringType(), False, {"comment": "Run configuration label"}),
    T.StructField("table_name",        T.StringType(), False, {"comment": "catalog.schema.table"}),
    T.StructField("rule_name",         T.StringType(), False, {"comment": "Rule name from config"}),
    T.StructField("severity",          T.StringType(), False, {"comment": "error | warning"}),
    T.StructField("rows_flagged",      T.LongType(),   False, {"comment": "Total rows flagged by this rule"}),
    T.StructField("table_total_rows",  T.LongType(),   True,  {"comment": "Total rows evaluated for table"}),
    T.StructField("pct_of_table_rows", T.DoubleType(), False, {"comment": "rows_flagged / table_total_rows"}),
])

CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT = T.StructType([
    T.StructField("run_config_name",          T.StringType(), False, {"comment": "Run configuration label"}),
    T.StructField("table_name",               T.StringType(), False, {"comment": "catalog.schema.table"}),
    T.StructField("table_total_rows",         T.LongType(),   False, {"comment": "count(*) evaluated"}),
    T.StructField("table_total_error_rows",   T.LongType(),   False, {"comment": "rows with non-empty _errors"}),
    T.StructField("table_total_warning_rows", T.LongType(),   False, {"comment": "rows with non-empty _warnings"}),
    T.StructField("total_flagged_rows",       T.LongType(),   False, {"comment": "rows with errors OR warnings"}),
    T.StructField("distinct_rules_fired",     T.IntegerType(),False, {"comment": "count_distinct(issue.name)"}),
])

# =======================================================================
# Helpers (inlined)
# =======================================================================
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
    return F.from_json(F.lit("[]"), T.ArrayType(ISSUE_STRUCT))

def normalize_issues_for_fp(col: F.Column) -> F.Column:
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

def coerce_arguments(expected: Dict[str, Any], provided: Optional[Dict[str, Any]], function_name: Optional[str], *, mode: str = "permissive") -> Tuple[Dict[str, Any], List[str]]:
    out: Dict[str, Any] = {}
    warns: List[str] = []
    provided = provided or {}
    for k, v in provided.items():
        if isinstance(v, (list, dict)):
            out[k] = F.lit(None)   # upstream usually stringifies
            warns.append(f"argument {k!r} is complex; ensure stringified upstream")
        else:
            out[k] = str(v) if v is not None else None
    return out, warns

def apply_rules_isolating_failures(dq_engine, src_df: DataFrame, rules: List[dict], table_name: str) -> Tuple[Optional[DataFrame], Optional[List[str]]]:
    if src_df is None:
        return None, ["source DataFrame is None"]
    annot = (
        src_df
        .withColumn("_errors",   empty_issues_array())
        .withColumn("_warnings", empty_issues_array())
    )
    return annot, None

# =======================================================================
# Utilities for rules & projection + diagnostics
# =======================================================================
def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for r in rules:
        out.setdefault(r["table_name"], []).append(r)
    return out

def _load_checks_from_table_as_dicts(
    spark: SparkSession,
    checks_table: str,
    run_config_name: str,
    coercion_mode: str = "permissive",
) -> Tuple[Dict[str, List[dict]], int, int, List[Dict[str, str]]]:
    df = (
        spark.table(checks_table)
        .where((F.col("run_config_name") == run_config_name) & (F.col("active") == True))
        .select("table_name", "name", "criticality", "filter", "run_config_name", "user_metadata", "check", "yaml_path")
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]

    raw_rules: List[dict] = []
    coerced: int = 0

    for r in rows:
        chk = r.get("check") or {}
        fn  = chk.get("function")
        args, _ = coerce_arguments(EXPECTED_ARGS, chk.get("arguments"), fn, mode=coercion_mode)
        coerced += 1
        raw_rules.append({
            "table_name": r["table_name"],
            "name": r["name"],
            "criticality": r["criticality"],
            "run_config_name": r["run_config_name"],
            "filter": r.get("filter"),
            "user_metadata": r.get("user_metadata"),
            "check": {"function": fn, "for_each_column": chk.get("for_each_column") or None, "arguments": args},
            "_yaml_path": r.get("yaml_path")
        })

    failures: List[Dict[str, str]] = []
    status = DQEngine.validate_checks(raw_rules)
    if getattr(status, "has_errors", False):
        keep: List[dict] = []
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if getattr(st, "has_errors", False):
                try:
                    reason = st.to_string()
                except Exception:
                    reason = "validation failed (no details available)"
                failures.append({
                    "table_name": str(r.get("table_name")),
                    "rule_name":  str(r.get("name")),
                    "severity":   str(r.get("criticality")),
                    "yaml_path":  str(r.get("_yaml_path") or ""),
                    "reason":     reason.strip()
                })
            else:
                keep.append(r)
        skipped = len(raw_rules) - len(keep)
        return _group_by_table(keep), coerced, skipped, failures

    return _group_by_table(raw_rules), coerced, 0, failures

def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _project_row_hits(
    df_annot: DataFrame,
    table_name: str,
    run_config_name: str,
    created_by: str,
    exclude_cols: Optional[List[str]] = None,
) -> DataFrame:
    exclude_cols = set(exclude_cols or [])
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errors_col   = F.col(e_name) if e_name else empty_issues_array()
    warnings_col = F.col(w_name) if w_name else empty_issues_array()

    df = (df_annot
          .withColumn("_errs", errors_col)
          .withColumn("_warns", warnings_col)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]
    row_snapshot = F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value")) for c in sorted(cols)])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(normalize_issues_for_fp(F.col("_warns")))), 256)
    log_id = F.sha2(F.concat_ws("||", F.lit(table_name), F.lit(run_config_name), row_snapshot_fp, _errors_fp, _warnings_fp), 256)

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

def _embed_issue_check_ids(row_log_df: DataFrame, checks_table: str) -> DataFrame:
    spark = row_log_df.sparkSession

    cfg_base = (
        spark.table(checks_table)
        .where(F.col("active") == True)
        .select(
            F.lower(F.col("table_name")).alias("t_tbl_norm"),
            F.col("run_config_name").alias("t_rc"),
            F.lower(F.trim(F.col("name"))).alias("t_name_norm"),
            F.lower(F.trim(F.coalesce(F.col("filter"), F.lit("")))).alias("t_filter_norm"),
            F.col("check_id").alias("cfg_check_id"),
        )
        .dropDuplicates(["t_tbl_norm","t_rc","t_name_norm","t_filter_norm","cfg_check_id"])
    )

    cfg_keys = (
        cfg_base
        .withColumn("arr", F.array(
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 3, F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -3), F.element_at(F.split("t_tbl_norm", r"\."), -2), F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.when(F.size(F.split("t_tbl_norm", r"\.")) >= 2, F.concat_ws(".", F.element_at(F.split("t_tbl_norm", r"\."), -2), F.element_at(F.split("t_tbl_norm", r"\."), -1))),
            F.element_at(F.split("t_tbl_norm", r"\."), -1)
        ))
        .withColumn("cfg_tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
        .drop("arr")
        .dropDuplicates(["cfg_tbl_key","t_rc","t_name_norm","t_filter_norm","cfg_check_id"])
    )

    issue_struct_type = ISSUE_STRUCT

    def enrich(colname: str) -> DataFrame:
        log_side = (
            row_log_df
            .select("log_id", "table_name", "run_config_name", F.posexplode_outer(F.col(colname)).alias("pos", "iss"))
            .withColumn("tbl_norm", F.lower(F.col("table_name")))
            .withColumn("rc", F.col("run_config_name"))
            .withColumn("name_norm", F.lower(F.trim(F.col("iss.name"))))
            .withColumn("filter_norm", F.lower(F.trim(F.coalesce(F.col("iss.filter"), F.lit("")))))
            .withColumn("arr", F.array(
                F.when(F.size(F.split("tbl_norm", r"\.")) >= 3, F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -3), F.element_at(F.split("tbl_norm", r"\."), -2), F.element_at(F.split("tbl_norm", r"\."), -1))),
                F.when(F.size(F.split("tbl_norm", r"\.")) >= 2, F.concat_ws(".", F.element_at(F.split("tbl_norm", r"\."), -2), F.element_at(F.split("tbl_norm", r"\."), -1))),
                F.element_at(F.split("tbl_norm", r"\."), -1)
            ))
            .withColumn("tbl_key", F.explode(F.expr("filter(arr, x -> x is not null)")))
            .drop("arr")
        )

        matched = (
            log_side.join(
                cfg_keys,
                (F.col("rc") == F.col("t_rc")) &
                (F.col("name_norm") == F.col("t_name_norm")) &
                (F.col("filter_norm") == F.col("t_filter_norm")) &
                (F.col("tbl_key") == F.col("cfg_tbl_key")),
                "left"
            )
            .groupBy("log_id", "pos")
            .agg(F.first("iss", ignorenulls=True).alias("iss"), F.max("cfg_check_id").alias("issue_check_id"))
        )

        return (
            matched
            .select(
                "log_id", "pos",
                F.to_json(F.struct(
                    F.col("iss.name").alias("name"),
                    F.col("iss.message").alias("message"),
                    F.col("iss.columns").alias("columns"),
                    F.col("iss.filter").alias("filter"),
                    F.col("iss.function").alias("function"),
                    F.col("iss.run_time").alias("run_time"),
                    F.col("iss.user_metadata").alias("user_metadata"),
                    F.col("issue_check_id").cast(T.StringType()).alias("check_id"),
                )).alias("iss_json")
            )
            .groupBy("log_id")
            .agg(F.array_sort(F.collect_list(F.struct(F.col("pos"), F.col("iss_json")))).alias("kv"))
            .withColumn(colname, F.transform(F.col("kv"), lambda x: F.from_json(x["iss_json"], issue_struct_type)))
            .select("log_id", colname)
        )

    err_arr = enrich("_errors"); warn_arr = enrich("_warnings")
    return (
        row_log_df.drop("_errors","_warnings")
        .join(err_arr, "log_id", "left")
        .join(warn_arr, "log_id", "left")
        .withColumn("_errors", F.coalesce(F.col("_errors"), empty_issues_array()))
        .withColumn("_warnings", F.coalesce(F.col("_warnings"), empty_issues_array()))
    )

def _summarize_table(annot: DataFrame, table_name: str) -> Row:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    annot = annot.cache()
    tot = annot.count()

    sums = (
        annot
        .select(
            (F.size(F.col(err)) > 0).cast("int").alias("e"),
            (F.size(F.col(wrn)) > 0).cast("int").alias("w"),
            ((F.size(F.col(err)) > 0) | (F.size(F.col(wrn)) > 0)).cast("int").alias("f"),
        )
        .agg(F.sum("e").alias("e"), F.sum("w").alias("w"), F.sum("f").alias("f"))
        .collect()[0]
    )

    names_arr = F.expr(f"array_union(transform({err}, x -> x.name), transform({wrn}, x -> x.name))")

    rules = (
        annot
        .select(F.explode_outer(names_arr).alias("nm"))
        .where(F.col("nm").isNotNull())
        .agg(F.countDistinct("nm").alias("rules"))
        .collect()[0]["rules"]
    )

    annot.unpersist()

    return Row(
        table_name=table_name,
        table_total_rows=int(tot),
        table_total_error_rows=int(sums["e"]),
        table_total_warning_rows=int(sums["w"]),
        total_flagged_rows=int(sums["f"]),
        distinct_rules_fired=int(rules),
    )

def _rules_hits_for_table(annot: DataFrame, table_name: str) -> DataFrame:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    errs = (annot.select(F.explode_outer(F.expr(f"transform({err}, x -> x.name)")).alias("name"))
                  .where(F.col("name").isNotNull()).withColumn("severity", F.lit("error")))
    warns = (annot.select(F.explode_outer(F.expr(f"transform({wrn}, x -> x.name)")).alias("name"))
                   .where(F.col("name").isNotNull()).withColumn("severity", F.lit("warning")))
    both = errs.unionByName(warns, allowMissingColumns=True)
    return both.groupBy("name","severity").agg(F.count(F.lit(1)).alias("rows_flagged")).withColumn("table_name", F.lit(table_name))

# =======================================================================
# Runner
# =======================================================================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive",
) -> Dict[str, Any]:

    nb = cfg.notebook(notebook_idx)

    # Resolve checks table from notebook_1.target_table_1
    checks_table = f'{cfg.get("notebooks.notebook_1.targets.target_table_1.catalog")}.{cfg.get("notebooks.notebook_1.targets.target_table_1.schema")}.{cfg.get("notebooks.notebook_1.targets.target_table_1.table")}'

    # Targets (3 tables)
    targets = nb.targets()
    t1 = targets.target_table(1)   # row log
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    tw = TableWriter(spark)
    apply_meta = bool(cfg.get("variables.apply_table_metadata"))

    # -- checks_log
    results_fqn   = t1.full_table_name()
    results_write = t1.get("write") or {}
    results_pk    = t1.get("primary_key")
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_tags  = t1.table_tags()   # <-- use normalized tags from config.py

    if not table_exists(spark, results_fqn):
        tw.create_table(
            fqn=results_fqn,
            schema=CHECKS_LOG_STRUCT,
            format=results_write.get("format"),
            options=results_write.get("options") or {},
            partition_by=results_part,
            table_comment=results_desc,
            table_tags=results_tags,
            primary_key_cols=[results_pk] if results_pk else None,
        )

    # -- summaries (optional)
    summary_by_rule_fqn = None
    if t2:
        summary_by_rule_fqn  = t2.full_table_name()
        t2_write = t2.get("write") or {}
        summary_by_rule_tags = t2.table_tags()  # <-- normalized
        if not table_exists(spark, summary_by_rule_fqn):
            tw.create_table(
                fqn=summary_by_rule_fqn,
                schema=CHECKS_LOG_SUMMARY_BY_RULE_STRUCT,
                format=t2_write.get("format"),
                options=t2_write.get("options") or {},
                partition_by=t2.get("partition_by") or [],
                table_comment=t2.get("table_description"),
                table_tags=summary_by_rule_tags,
                primary_key_cols=[t2.get("primary_key")] if t2.get("primary_key") else None,
            )

    summary_by_table_fqn = None
    if t3:
        summary_by_table_fqn  = t3.full_table_name()
        t3_write = t3.get("write") or {}
        summary_by_table_tags = t3.table_tags()  # <-- normalized
        if not table_exists(spark, summary_by_table_fqn):
            tw.create_table(
                fqn=summary_by_table_fqn,
                schema=CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT,
                format=t3_write.get("format"),
                options=t3_write.get("options") or {},
                partition_by=t3.get("partition_by") or [],
                table_comment=t3.get("table_description"),
                table_tags=summary_by_table_tags,
                primary_key_cols=[t3.get("primary_key")] if t3.get("primary_key") else None,
            )

    # DQ Engine
    dq = DQEngine(WorkspaceClient())

    # information only
    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []
    printed_grand_once = False

    rc_map: Dict[str, Any] = cfg.get("run_config_name") or {}

    for rc_name, _rc_cfg in sorted(rc_map.items(), key=lambda kv: str(kv[0])):
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        display_section(f"Run config: {rc_name}")

        by_tbl, coerced, skipped, failures = _load_checks_from_table_as_dicts(
            spark, checks_table, rc_name, coercion_mode=coercion_mode
        )
        checks_loaded = sum(len(v) for v in by_tbl.values())

        print(f"[{rc_name}] checks_in_table_total={checks_table_total}, loaded={checks_loaded}, coerced={coerced}, skipped_invalid={skipped}")

        if failures:
            fail_df = spark.createDataFrame(
                [Row(run_config_name=rc_name, **f) for f in failures],
                schema="run_config_name string, table_name string, rule_name string, severity string, yaml_path string, reason string",
            )
            display_section(f"Skipped rules (validation errors) — run_config={rc_name}")
            show_df(fail_df.orderBy("table_name","rule_name"), n=1000, truncate=False)

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
                annot, bad = apply_rules_isolating_failures(dq, src, tbl_rules, tbl)
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

            out_batches.append(_project_row_hits(annot, tbl, rc_name, created_by="AdminUser", exclude_cols=exclude_cols))

        if rc_tbl_summaries:
            summary_df = spark.createDataFrame(rc_tbl_summaries).orderBy("table_name")
            display_section(f"Row-hit summary by table (run_config={rc_name})")
            show_df(summary_df, n=200, truncate=False)

        # summary by rule
        if rc_rule_hit_parts and summary_by_rule_fqn:
            rules_all = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), rc_rule_hit_parts)
            cfg_rules = (
                spark.table(checks_table)
                .where((F.col("run_config_name") == rc_name) & (F.col("active") == True))
                .where(F.col("table_name").isin(processed_tables))
                .select(
                    F.col("table_name"),
                    F.col("name").alias("rule_name"),
                    F.when(F.lower("criticality").isin("warn","warning"), F.lit("warning")).otherwise(F.lit("error")).alias("severity"),
                )
                .dropDuplicates(["table_name","rule_name","severity"])
            )
            counts = (rules_all.groupBy("table_name","name","severity")
                               .agg(F.sum("rows_flagged").alias("rows_flagged"))
                               .withColumnRenamed("name","rule_name"))
            full_rules = (cfg_rules.join(counts, on=["table_name","rule_name","severity"], how="left")
                                   .withColumn("rows_flagged", F.coalesce(F.col("rows_flagged"), F.lit(0))))
            totals_df = spark.createDataFrame([Row(table_name=k, table_total_rows=v) for k, v in table_row_counts.items()])
            full_rules = (
                full_rules.join(totals_df, "table_name", "left")
                .withColumn("pct_of_table_rows",
                            F.when(F.col("table_total_rows") > 0,
                                   F.col("rows_flagged") / F.col("table_total_rows")).otherwise(F.lit(0.0)))
                .select("table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
                .orderBy(F.desc("rows_flagged"), F.asc("table_name"), F.asc("rule_name"))
            )
            display_section(f"Row-hit summary by rule (run_config={rc_name})")
            show_df(full_rules, n=2000, truncate=False)

            t2_write = t2.get("write") if t2 else {}
            (full_rules
             .withColumn("run_config_name", F.lit(rc_name))
             .select("run_config_name","table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
             .write
             .format(t2_write.get("format"))
             .mode(t2_write.get("mode"))
             .options(**(t2_write.get("options") or {}))
             .saveAsTable(summary_by_rule_fqn))

        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), out_batches)

        out = _embed_issue_check_ids(out, checks_table)

        # Enforce PK idempotency before write
        pk_cols = [results_pk] if isinstance(results_pk, str) else list(results_pk)
        dupes = out.groupBy(*pk_cols).count().where(F.col("count") > 1)
        if dupes.limit(1).count() > 0:
            raise RuntimeError(f"results batch contains duplicate PKs: {pk_cols}")
        out = out.dropDuplicates(pk_cols)

        # Write (no alignment helper)
        tw.write_df(
            df=out,
            fqn=results_fqn,
            mode=results_write.get("mode"),
            format=results_write.get("format"),
            options=results_write.get("options") or {},
        )

        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # summary by table (ALL run_configs)
        if summary_by_table_fqn:
            grand_df = (
                spark.createDataFrame(all_tbl_summaries)
                .select("run_config_name","table_name","table_total_rows","table_total_error_rows",
                        "table_total_warning_rows","total_flagged_rows","distinct_rules_fired")
                .orderBy("run_config_name","table_name")
            )
            if not printed_grand_once:
                display_section("Row-hit summary by table (ALL run_configs)")
                show_df(grand_df, n=500, truncate=False)
                printed_grand_once = True

            t3_write = t3.get("write") if t3 else {}
            (grand_df
             .write
             .format(t3_write.get("format"))
             .mode(t3_write.get("mode"))
             .options(**(t3_write.get("options") or {}))
             .saveAsTable(summary_by_table_fqn))

    display_section("Grand total")
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

    return {"results_table": results_fqn, "grand_total_rows": grand_total, "checks_table": checks_table, "notebook_idx": notebook_idx}

# -------------------------
# Entrypoint (local/dev)
# -------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    show_notebook_env(spark)
    cfg = ProjectConfig("resources/dqx_config.yaml")
    result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2, coercion_mode="strict")
    print(result)

# COMMAND ----------


