# Databricks notebook source
# MAGIC %md
# MAGIC # Run DQX Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference

# COMMAND ----------

# DBTITLE 1,Runbook
"""
### Runbook — 02_run_dqx_checks (Run DQX Checks)

**Purpose**  
Execute Databricks Labs DQX rules from the config table, write a row‑level results log, and produce lightweight rollups for dashboards.

---

## At a glance
- **Notebook:** `02_run_dqx_checks`
- **Engine:** `databricks-labs-dqx==0.8.x`
- **Reads:** rules from `dq_{env}.dqx.checks_log` (Delta)
- **Writes:**
  - **Row log:** `dqx_checks_log_table_name` (Delta, one row per flagged source row)
  - **Summary (by rule):** `dq_dev.dqx.checks_log_summary_by_rule` (append)
  - **Summary (by table):** `dq_dev.dqx.checks_log_summary_by_table` (overwrite)
- **Key behavior:** embeds `check_id` **inside each issue element** of `_errors`/`_warnings` (no top‑level `check_id` column). Deterministic `log_id` prevents accidental duplicates within a run.

---

## Prerequisites
- Unity Catalog schemas exist and you have permission to **read source tables** and **write** to the targets above.
- Rules have been loaded by **01_load_dqx_checks** into the checks config table.
- Cluster/SQL warehouse with Delta support. The notebook enables `spark.databricks.delta.schema.autoMerge.enabled=true` for nested struct evolution.

---

## Inputs & Config
YAML at `resources/dqx_config.yaml` (or your path) must define:
```yaml
dqx_checks_config_table_name: dq_dev.dqx.checks           # rules table (from loader)
dqx_checks_log_table_name:    dq_dev.dqx.checks_log       # results (row log)
run_config_name:                                          # named run configs (RCs)
  nightly:
    output_config:
      mode: overwrite                                     # overwrite|append
      options: {}                                         # any DataFrameWriter options
  ad_hoc:
    output_config:
      mode: append
      options: {}
```

**Rules selection per RC:** active rules where `run_config_name == <RC>` are loaded from the checks table.

---

## How to run
Minimal usage in a cell:
```python
# One‑time per cluster session
# %pip install databricks-labs-dqx==0.8.0
# dbutils.library.restartPython()

run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    time_zone="America/Chicago",
    exclude_cols=None,            # optional: columns to exclude from row_snapshot
    coercion_mode="strict"        # "permissive" or "strict" argument coercion
)
```

**Parameters**
- `dqx_cfg_yaml` – path to the YAML above.
- `created_by` – audit field written to the row log.
- `time_zone` – for banners/printing only.
- `exclude_cols` – columns to omit from `row_snapshot` (e.g., large blobs).
- `coercion_mode` – how strictly to coerce stringified rule arguments (`strict` recommended).

---

## What it does (flow)
1. **Environment banner** and YAML load.  
2. **Ensure row‑log table exists** (creates empty Delta table with schema & comments if missing).  
3. For each **run config (RC)** in YAML:  
   a. **Load active rules** for that RC from the checks table; JIT‑coerce argument types; validate with `DQEngine`. Invalid rules are skipped (isolated).  
   b. **Group by table**, read each source table, and call `DQEngine.apply_checks_by_metadata`. If a batch fails, the runner tests rules individually and drops offenders.  
   c. **Project row hits** to the row‑log schema, computing:
      - `row_snapshot` = `[{"column","value"}...]` over non‑reserved columns (stringified)  
      - Fingerprints for `_errors`, `_warnings`, and `row_snapshot`  
      - Deterministic `log_id = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp)`  
   d. **Embed `check_id`** into each issue element (errors & warnings) by matching `(table, run_config_name, rule name, filter)`.  
   e. **Write row log** to `dqx_checks_log_table_name` using RC’s `output_config` (default `overwrite`).  
   f. **Summaries:** display per‑table rollups; build **by‑rule** counts (including zero‑hit rules) and append to `dq_dev.dqx.checks_log_summary_by_rule`.  
4. **Grand rollup (all RCs)** is written to `dq_dev.dqx.checks_log_summary_by_table` (overwrite).  
5. **Final banner** prints total rows written.

---

## Row‑Log schema (essential fields)
- `log_id` *(PK)* – deterministic hash; ensures idempotency within a run.  
- `table_name`, `run_config_name` – lineage.  
- `_errors`, `_warnings` – arrays of issue structs:  
  `{ name, message, columns[], filter, function, run_time, user_metadata, check_id }`  
- `_errors_fingerprint`, `_warnings_fingerprint` – order/column‑order insensitive digests.  
- `row_snapshot` – array of `{column, value}` (stringified).  
- `row_snapshot_fingerprint` – digest included in `log_id`.  
- `created_by`, `created_at`, `updated_by`, `updated_at` – audit.

> **Note:** There is **no** top‑level `check_id` column; IDs live **inside** each issue element.

---

## Operational tips
- **Write mode per RC**: control idempotency via YAML (`overwrite` recommended for scheduled runs; `append` for incremental scenarios).  
- **Schema evolution**: if upgrading from a version without `issue.check_id`, the runner sets `autoMerge=true` to evolve arrays of structs in place.  
- **Performance**: large tables can be slow—filter with rule‑level `filter` predicates in the checks table; consider running by table RCs.  
- **Dashboards**: the downstream views in your dashboard should explode `_errors`/`_warnings` and join by embedded `check_id` when needed.

---

## Troubleshooting
- **“no checks loaded”** for an RC → ensure checks table has `active=true` and exact `run_config_name` match.  
- **Validation failures** → the runner will isolate bad rules and continue; check notebook output for offending rules (JSON logged). Fix the rule in YAML and re‑load with the loader.  
- **Duplicate rows in append mode** → `log_id` is deterministic for the same row snapshot & issues; if inputs change (e.g., timestamps), duplicates are expected. Prefer `overwrite` for repeatable runs.  
- **Missing `check_id` in issues** → verify rule names/filters/table names in checks table match the issues; embedding derives IDs by `(table_key, run_config_name, rule name, filter)`.

---

## Verification queries
```sql
-- Row log rows by run config
SELECT run_config_name, COUNT(*) AS rows
FROM dq_dev.dqx.checks_log
GROUP BY run_config_name
ORDER BY rows DESC;

-- Any issue elements missing check_id?
SELECT SUM(size(filter(_errors,   x -> x.check_id IS NULL))) AS errors_missing_id,
       SUM(size(filter(_warnings, x -> x.check_id IS NULL))) AS warnings_missing_id
FROM dq_dev.dqx.checks_log;

-- Latest summaries
SELECT * FROM dq_dev.dqx.checks_log_summary_by_rule   ORDER BY run_config_name, rows_flagged DESC;
SELECT * FROM dq_dev.dqx.checks_log_summary_by_table  ORDER BY run_config_name, table_name;
```
"""

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

# DBTITLE 1,DQX Result Schema
#NOTE: https://github.com/databrickslabs/dqx/blob/v0.9.0/src/databricks/labs/dqx/schema/dq_result_schema.py
"""
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, TimestampType, MapType

dq_result_item_schema = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("message", StringType(), nullable=True),
        StructField("columns", ArrayType(StringType()), nullable=True),
        StructField("filter", StringType(), nullable=True),
        StructField("function", StringType(), nullable=True),
        StructField("run_time", TimestampType(), nullable=True),
        StructField("user_metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)

dq_result_schema = ArrayType(dq_result_item_schema)
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

# DBTITLE 1,Configure Python Path for Framework Module Import
import sys
from pathlib import Path

def add_src_to_sys_path(src_dir="src", sentinel="framework_utils", max_levels=12):
    start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    p = start
    for _ in range(max_levels):
        cand = p / src_dir
        if (cand / sentinel).exists():
            s = str(cand.resolve())
            if s not in sys.path:
                sys.path.insert(0, s)
                print(f"[bootstrap] sys.path[0] = {s}")
            return
        if p == p.parent: break
        p = p.parent
    raise ImportError(f"Couldn't find {src_dir}/{sentinel} above {start}")

add_src_to_sys_path()

# COMMAND ----------

# DBTITLE 1,Main
# Databricks notebook: 02_run_dqx_checks
# Purpose: Run DQX checks and write row-level logs + summaries
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
import sys
import json
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

"""
def add_src_to_sys_path(src_dir="src", sentinel="framework_utils", max_levels=12):
    start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    p = start
    for _ in range(max_levels):
        cand = p / src_dir
        if (cand / sentinel).exists():
            s = str(cand.resolve())
            if s not in sys.path:
                sys.path.insert(0, s)
                print(f"[bootstrap] sys.path[0] = {s}")
            return
        if p == p.parent: break
        p = p.parent
    raise ImportError(f"Couldn't find {src_dir}/{sentinel} above {start}")

add_src_to_sys_path()
"""

from utils.color import Color
from utils.runtime import show_notebook_env
from utils.display import show_df, display_section
from utils.config import ProjectConfig
from utils.write import TableWriter
from utils.table import table_exists

# =====================================================================================
# Target table schemas (unchanged)
# =====================================================================================
CHECKS_LOG_STRUCT = T.StructType([
    T.StructField("log_id",                  T.StringType(),   False),
    T.StructField("table_name",              T.StringType(),   False),
    T.StructField("run_config_name",         T.StringType(),   False),

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
    T.StructField("_errors_fingerprint",     T.StringType(),   False),

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
    T.StructField("_warnings_fingerprint",   T.StringType(),   False),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",        T.StringType(), False),
        T.StructField("value",         T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint",T.StringType(),   False),

    T.StructField("created_by",              T.StringType(),   False),
    T.StructField("created_at",              T.TimestampType(),False),
    T.StructField("updated_by",              T.StringType(),   True),
    T.StructField("updated_at",              T.TimestampType(),True),
])

CHECKS_LOG_SUMMARY_BY_RULE_STRUCT = T.StructType([
    T.StructField("run_config_name",   T.StringType(), False),
    T.StructField("table_name",        T.StringType(), False),
    T.StructField("rule_name",         T.StringType(), False),
    T.StructField("severity",          T.StringType(), False),
    T.StructField("rows_flagged",      T.LongType(),   False),
    T.StructField("table_total_rows",  T.LongType(),   True),
    T.StructField("pct_of_table_rows", T.DoubleType(), False),
])

CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT = T.StructType([
    T.StructField("run_config_name",          T.StringType(), False),
    T.StructField("table_name",               T.StringType(), False),
    T.StructField("table_total_rows",         T.LongType(),   False),
    T.StructField("table_total_error_rows",   T.LongType(),   False),
    T.StructField("table_total_warning_rows", T.LongType(),   False),
    T.StructField("total_flagged_rows",       T.LongType(),   False),
    T.StructField("distinct_rules_fired",     T.IntegerType(),False),
])

# =====================================================================================
# Issue shapes
#   - RAW = exactly what DQX emits (NO check_id)
#   - WITH-ID = same + check_id (we add only before writing)
# =====================================================================================
DQX_ISSUE_STRUCT = T.StructType([
    T.StructField("name",          T.StringType(), True),
    T.StructField("message",       T.StringType(), True),
    T.StructField("columns",       T.ArrayType(T.StringType()), True),
    T.StructField("filter",        T.StringType(), True),
    T.StructField("function",      T.StringType(), True),
    T.StructField("run_time",      T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
])
DQX_ISSUE_ARRAY = T.ArrayType(DQX_ISSUE_STRUCT)

ISSUE_WITH_ID_STRUCT = T.StructType(DQX_ISSUE_STRUCT.fields + [T.StructField("check_id", T.StringType(), True)])
ISSUE_WITH_ID_ARRAY = T.ArrayType(ISSUE_WITH_ID_STRUCT)

def _empty_raw_issues() -> F.Column:
    return F.from_json(F.lit("[]"), DQX_ISSUE_ARRAY)

def _empty_with_id_issues() -> F.Column:
    return F.from_json(F.lit("[]"), ISSUE_WITH_ID_ARRAY)

def _normalize_to_raw_issue_array(col: F.Column) -> F.Column:
    js = F.when(col.isNull(), F.lit("[]")).otherwise(F.to_json(col))
    return F.coalesce(F.from_json(js, DQX_ISSUE_ARRAY), _empty_raw_issues())

# =====================================================================================
# Small JIT coercion to turn table strings into real lists/numbers/dicts
# (enough to fix the DQX validation complaints you showed)
# =====================================================================================
def _parse_scalar(v):
    if v is None:
        return None
    if isinstance(v, (dict, list, bool, int, float)):
        return v
    s = str(v).strip()
    if s.lower() in ("", "null", "none"):
        return None
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    # JSON-ish containers
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try:
            return json.loads(s)
        except Exception:
            return s
    # numbers
    try:
        if s.lstrip("+-").isdigit():
            return int(s)
        return float(s)
    except Exception:
        return s

def _coerce_arguments(args: Optional[dict]) -> dict:
    if args is None:
        return {}  # DQX expects a dict; {} is fine for functions with no args
    return {k: _parse_scalar(v) for k, v in args.items()}

# =====================================================================================
# Load checks FROM TABLE (active + specific run_config_name); validate via DQX
# =====================================================================================
def _load_checks_from_table(spark: SparkSession, checks_table: str, run_config_name: str) -> Tuple[List[dict], List[dict]]:
    df = (
        spark.table(checks_table)
        .where( (F.col("active") == True) & (F.col("run_config_name") == F.lit(run_config_name)) )
        .select("table_name","name","criticality","filter","user_metadata","check","yaml_path","check_id")
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]

    rules: List[dict] = []
    for r in rows:
        chk = r.get("check") or {}
        rules.append({
            "table_name":     r["table_name"],
            "name":           r["name"],
            "criticality":    r["criticality"],
            "filter":         r.get("filter"),
            "user_metadata":  r.get("user_metadata"),
            "yaml_path":      r.get("yaml_path"),
            "check_id":       r.get("check_id"),
            "check": {
                "function":        chk.get("function"),
                "for_each_column": chk.get("for_each_column"),
                "arguments":       _coerce_arguments(chk.get("arguments")),
            }
        })

    # Validate each rule so we can report exactly which ones are bad
    dq = DQEngine(WorkspaceClient())
    good, bad = [], []
    for r in rules:
        st = DQEngine.validate_checks([{
            "name": r["name"],
            "criticality": r["criticality"],
            "filter": r.get("filter"),
            "user_metadata": r.get("user_metadata"),
            "check": r["check"],
        }])
        if getattr(st, "has_errors", False):
            reason = ""
            try:
                reason = st.to_string()
            except Exception:
                reason = "validation failed"
            br = dict(r)
            br["_reason"] = reason
            bad.append(br)
        else:
            good.append(r)
    return good, bad

# =====================================================================================
# Apply DQX (batch first, then isolate failing rules)
# =====================================================================================
def _apply_by_metadata(dq: DQEngine, src_df: DataFrame, checks: List[dict], table_name: str) -> Tuple[Optional[DataFrame], Optional[str]]:
    if not checks:
        return None, "no valid checks"
    try:
        annot = dq.apply_checks_by_metadata(src_df, [
            {k: v for k, v in c.items() if k in {"name","criticality","filter","user_metadata","check"}}
            for c in checks
        ])
    except Exception as e:
        return None, f"{table_name}: dq_engine.apply_checks_by_metadata failed: {e}"
    return annot, None

def _isolate_bad_rules(dq: DQEngine, src_df: DataFrame, rules: List[dict], table_name: str) -> Tuple[List[dict], List[dict]]:
    good, bad = [], []
    for r in rules:
        try:
            dq.apply_checks_by_metadata(src_df, [{k: v for k, v in r.items() if k in {"name","criticality","filter","user_metadata","check"}}])
            good.append(r)
        except Exception as e:
            rr = dict(r); rr["_reason"] = f"{table_name}: rule '{r.get('name')}' failed: {e}"
            bad.append(rr)
    return good, bad

# =====================================================================================
# Row projection (RAW issue arrays; add check_id later)
# =====================================================================================
def _pick_col(df: DataFrame, *cands: str) -> Optional[str]:
    for c in cands:
        if c in df.columns:
            return c
    return None

def _normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # order-insensitive fingerprint: drop non-deterministic fields, sort columns
    return F.transform(
        arr_col,
        lambda r: F.struct(
            F.lower(F.trim(r["name"])).alias("name"),
            F.trim(r["message"]).alias("message"),
            F.array_sort(
                F.transform(F.coalesce(r["columns"], F.array().cast(T.ArrayType(T.StringType()))),
                            lambda c: F.lower(F.trim(c)))
            ).alias("columns"),
            F.lower(F.trim(r["filter"])).alias("filter"),
            F.lower(F.trim(r["function"])).alias("function"),
        )
    )

def _project_row_hits(df_annot: DataFrame, table_name: str, run_config_name: str, created_by: str, exclude_cols: Optional[List[str]] = None) -> DataFrame:
    exclude_cols = set(exclude_cols or [])
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errs = _normalize_to_raw_issue_array(F.col(e_name) if e_name else F.lit(None))
    wrns = _normalize_to_raw_issue_array(F.col(w_name) if w_name else F.lit(None))

    df = (df_annot
          .withColumn("_errs", errs)
          .withColumn("_warns", wrns)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]

    row_snapshot = F.array(*[
        F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value"))
        for c in sorted(cols)
    ])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_warns")))), 256)

    log_id = F.sha2(
        F.concat_ws("||", F.lit(table_name), F.lit(run_config_name), row_snapshot_fp, _errors_fp, _warnings_fp),
        256
    )

    return df.select(
        log_id.alias("log_id"),
        F.lit(table_name).alias("table_name"),
        F.lit(run_config_name).alias("run_config_name"),
        F.col("_errs").alias("_errors"),     # RAW (no check_id)
        _errors_fp.alias("_errors_fingerprint"),
        F.col("_warns").alias("_warnings"),  # RAW (no check_id)
        _warnings_fp.alias("_warnings_fingerprint"),
        row_snapshot.alias("row_snapshot"),
        row_snapshot_fp.alias("row_snapshot_fingerprint"),
        F.lit(created_by).alias("created_by"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast(T.StringType()).alias("updated_by"),
        F.lit(None).cast(T.TimestampType()).alias("updated_at"),
    )

# =====================================================================================
# Embed check_id (RAW → WITH-ID) without sorting nested structs
# =====================================================================================
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

    # final issue struct we want (WITH check_id)
    issue_struct_type = ISSUE_WITH_ID_STRUCT

    def enrich(colname: str) -> DataFrame:
        log_side = (
            row_log_df
            .select("log_id", "table_name", "run_config_name", F.posexplode_outer(F.col(colname)).alias("pos","iss"))
            .withColumn("tbl_norm", F.lower(F.col("table_name")))
            .withColumn("rc", F.col("run_config_name"))
            .withColumn("name_norm",   F.lower(F.trim(F.col("iss.name"))))
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
            log_side.join(
                cfg_keys,
                (F.col("rc") == F.col("t_rc")) &
                (F.col("name_norm") == F.col("t_name_norm")) &
                (F.col("filter_norm") == F.col("t_filter_norm")) &
                (F.col("tbl_key") == F.col("cfg_tbl_key")),
                "left"
            )
            .groupBy("log_id", "pos")
            .agg(F.first("iss", ignorenulls=True).alias("iss"),
                 F.max("cfg_check_id").alias("issue_check_id"))
        )

        # Build JSON so we can sort safely (pos:int, iss_json:string)
        matched_json = matched.select(
            "log_id", "pos",
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

        return (
            matched_json
            .groupBy("log_id")
            .agg(F.sort_array(F.collect_list(F.struct(F.col("pos"), F.col("iss_json")))).alias("kv"))
            .withColumn(colname, F.transform(F.col("kv"), lambda x: F.from_json(x["iss_json"], issue_struct_type)))
            .select("log_id", colname)
        )

    err_arr = enrich("_errors")
    warn_arr = enrich("_warnings")

    return (
        row_log_df.drop("_errors","_warnings")
        .join(err_arr,  "log_id", "left")
        .join(warn_arr, "log_id", "left")
        .withColumn("_errors",   F.coalesce(F.col("_errors"),   _empty_with_id_issues()))
        .withColumn("_warnings", F.coalesce(F.col("_warnings"), _empty_with_id_issues()))
    )

# =====================================================================================
# Summaries
# =====================================================================================
def _summarize_table(annot: DataFrame, table_name: str) -> Row:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"
    annot = annot.cache()
    tot = annot.count()
    sums = (annot
        .select(
            (F.size(F.col(err)) > 0).cast("int").alias("e"),
            (F.size(F.col(wrn)) > 0).cast("int").alias("w"),
            ((F.size(F.col(err)) > 0) | (F.size(F.col(wrn)) > 0)).cast("int").alias("f"),
        )
        .agg(F.sum("e").alias("e"), F.sum("w").alias("w"), F.sum("f").alias("f"))
        .collect()[0]
    )
    names_arr = F.expr(f"array_union(transform({err}, x -> x.name), transform({wrn}, x -> x.name))")
    rules = (annot
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

# =====================================================================================
# Runner
# =====================================================================================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive"  # left here if you later want to tighten
) -> Dict[str, Any]:

    # keep UTC in session; your env printer shows local too
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    nb = cfg.notebook(notebook_idx)

    # checks table = notebook_1 target_table_1
    checks_table = f'{cfg.get("notebooks.notebook_1.targets.target_table_1.catalog")}.{cfg.get("notebooks.notebook_1.targets.target_table_1.schema")}.{cfg.get("notebooks.notebook_1.targets.target_table_1.table")}'

    # results table T1
    targets = nb.targets()
    t1 = targets.target_table(1)
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    tw = TableWriter(spark)

    # Create results table if missing
    results_fqn   = t1.full_table_name()
    results_write = t1.get("write") or {}
    results_pk    = t1.get("primary_key")
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_tags  = t1.table_tags()

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

    # Create optional summary tables if missing
    summary_by_rule_fqn = None
    if t2 and not table_exists(spark, t2.full_table_name()):
        tw.create_table(
            fqn=t2.full_table_name(),
            schema=CHECKS_LOG_SUMMARY_BY_RULE_STRUCT,
            format=(t2.get("write") or {}).get("format"),
            options=(t2.get("write") or {}).get("options") or {},
            partition_by=t2.get("partition_by") or [],
            table_comment=t2.get("table_description"),
            table_tags=t2.table_tags(),
            primary_key_cols=[t2.get("primary_key")] if t2.get("primary_key") else None,
        )
    if t2:
        summary_by_rule_fqn = t2.full_table_name()

    summary_by_table_fqn = None
    if t3 and not table_exists(spark, t3.full_table_name()):
        tw.create_table(
            fqn=t3.full_table_name(),
            schema=CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT,
            format=(t3.get("write") or {}).get("format"),
            options=(t3.get("write") or {}).get("options") or {},
            partition_by=t3.get("partition_by") or [],
            table_comment=t3.get("table_description"),
            table_tags=t3.table_tags(),
            primary_key_cols=[t3.get("primary_key")] if t3.get("primary_key") else None,
        )
    if t3:
        summary_by_table_fqn = t3.full_table_name()

    dq = DQEngine(WorkspaceClient())

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []

    # Discover RCs from checks table (active only)
    rc_names = [r["run_config_name"] for r in (
        spark.table(checks_table)
        .where(F.col("active") == True)
        .select("run_config_name").dropDuplicates().collect()
    ) if r["run_config_name"]]

    for rc_name in sorted(rc_names):
        display_section(f"Run config: {rc_name}")

        # Load + validate
        valid_checks, invalid_checks = _load_checks_from_table(spark, checks_table, rc_name)
        print(f"[{rc_name}] checks_in_table_total={checks_table_total}, loaded={len(valid_checks)}, skipped_invalid={len(invalid_checks)}")

        if invalid_checks:
            fail_df = spark.createDataFrame(
                [Row(run_config_name=rc_name,
                     table_name=str(c.get("table_name")),
                     rule_name=str(c.get("name")),
                     severity=str(c.get("criticality")),
                     yaml_path=str(c.get("yaml_path", "")),
                     reason=str(c.get("_reason","")).strip()) for c in invalid_checks],
                schema="run_config_name string, table_name string, rule_name string, severity string, yaml_path string, reason string",
            )
            display_section(f"Skipped rules (validation errors) — run_config={rc_name}")
            show_df(fail_df.orderBy("table_name","rule_name"), n=1000, truncate=False)

        if not valid_checks:
            print(f"[{rc_name}] no valid checks after validation.")
            continue

        # Group by table_name
        by_tbl: Dict[str, List[dict]] = {}
        for chk in valid_checks:
            by_tbl.setdefault(chk["table_name"], []).append(chk)

        out_batches: List[DataFrame] = []
        rc_tbl_summaries: List[Row] = []
        rc_rule_hit_parts: List[DataFrame] = []
        table_row_counts: Dict[str, int] = {}
        processed_tables: List[str] = []

        for tbl, tbl_rules in sorted(by_tbl.items()):
            try:
                src = spark.read.table(tbl)
            except Exception as e:
                print(f"[{rc_name}] {tbl} read failed: {e}")
                continue

            annot, err = _apply_by_metadata(dq, src, tbl_rules, tbl)
            if annot is None:
                good_rules, bad_rules = _isolate_bad_rules(dq, src, tbl_rules, tbl)
                if bad_rules:
                    bad_df = spark.createDataFrame(
                        [Row(run_config_name=rc_name,
                             table_name=tbl,
                             rule_name=str(r.get("name")),
                             severity=str(r.get("criticality")),
                             yaml_path=str(r.get("yaml_path","")),
                             reason=str(r.get("_reason","")).strip()) for r in bad_rules],
                        schema="run_config_name string, table_name string, rule_name string, severity string, yaml_path string, reason string",
                    )
                    display_section(f"Skipped rules (apply errors) — run_config={rc_name}, table={tbl}")
                    show_df(bad_df.orderBy("rule_name"), n=1000, truncate=False)

                if not good_rules:
                    print(f"[{rc_name}] {tbl} skipped — no good rules after isolation. Original error: {err}")
                    continue

                annot, err = _apply_by_metadata(dq, src, good_rules, tbl)
                if annot is None:
                    print(f"[{rc_name}] {tbl} skipped — re-apply with good rules failed: {err}")
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

        # Summary by rule (optional)
        if rc_rule_hit_parts and t2 and summary_by_rule_fqn:
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
                .withColumn("run_config_name", F.lit(rc_name))
                .select("run_config_name","table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
            )
            display_section(f"Row-hit summary by rule (run_config={rc_name})")
            show_df(full_rules, n=2000, truncate=False)

            t2_write = t2.get("write") or {}
            (full_rules
             .write
             .format(t2_write.get("format"))
             .mode(t2_write.get("mode"))
             .options(**(t2_write.get("options") or {}))
             .saveAsTable(summary_by_rule_fqn))

        # Row-level writes
        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), out_batches)

        # Embed check_id → WITH-ID arrays (and fix types to match CHECKS_LOG_STRUCT)
        out = _embed_issue_check_ids(out, checks_table)

        # Idempotency check on PK
        pk_cols = [t1.get("primary_key")] if isinstance(t1.get("primary_key"), str) else list(t1.get("primary_key"))
        if pk_cols:
            dupes = out.groupBy(*pk_cols).count().where(F.col("count") > 1)
            if dupes.limit(1).count() > 0:
                raise RuntimeError(f"results batch contains duplicate PKs: {pk_cols}")
            out = out.dropDuplicates(pk_cols)

        # Persist detailed row log
        t1_write = t1.get("write") or {}
        tw.write_df(
            df=out.select([f.name for f in CHECKS_LOG_STRUCT.fields]),
            fqn=results_fqn,
            mode=t1_write.get("mode"),
            format=t1_write.get("format"),
            options=t1_write.get("options") or {},
        )

        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # Summary by table (grand)
        if t3 and summary_by_table_fqn and all_tbl_summaries:
            grand_df = (
                spark.createDataFrame(all_tbl_summaries)
                .select("run_config_name","table_name","table_total_rows","table_total_error_rows",
                        "table_total_warning_rows","total_flagged_rows","distinct_rules_fired")
                .orderBy("run_config_name","table_name")
            )
            target_schema = spark.table(summary_by_table_fqn).schema
            grand_df_aligned = grand_df.select(
                *[F.col(f.name).cast(f.dataType).alias(f.name) for f in target_schema.fields]
            )
            t3_write = t3.get("write") or {}
            (grand_df_aligned
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
    show_notebook_env(spark)
    cfg = ProjectConfig("resources/dqx_config.yaml")
    result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2)
    print(result)

# COMMAND ----------

# DBTITLE 1,Testing
# Databricks notebook: 02_run_dqx_checks
# Purpose: Run DQX checks and write row-level logs + summaries
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
import sys
import json
from pathlib import Path
from functools import reduce
from typing import Dict, Any, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.engine import DQEngine

from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED

def add_src_to_sys_path(src_dir="src", sentinel="framework_utils", max_levels=12):
    start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    p = start
    for _ in range(max_levels):
        cand = p / src_dir
        if (cand / sentinel).exists():
            s = str(cand.resolve())
            if s not in sys.path:
                sys.path.insert(0, s)
                print(f"[bootstrap] sys.path[0] = {s}")
            return
        if p == p.parent: break
        p = p.parent
    raise ImportError(f"Couldn't find {src_dir}/{sentinel} above {start}")

add_src_to_sys_path()

from framework_utils.color import Color
from framework_utils.runtime import show_notebook_env
from framework_utils.display import show_df, display_section
from framework_utils.config import ProjectConfig
from framework_utils.write import TableWriter
from framework_utils.table import table_exists


# =====================================================================================
# Target table schemas (comments embedded like Notebook 1)
# =====================================================================================
CHECKS_LOG_STRUCT = T.StructType([
    T.StructField("log_id", T.StringType(), False, {
        "comment": "PRIMARY KEY. sha256(table_name, run_config_name, row_snapshot_fingerprint, _errors_fingerprint, _warnings_fingerprint)."
    }),
    T.StructField("table_name", T.StringType(), False, {
        "comment": "Source table FQN (catalog.schema.table) evaluated by DQX."
    }),
    T.StructField("run_config_name", T.StringType(), False, {
        "comment": "Run configuration under which the checks were executed."
    }),

    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name", T.StringType(), True, {"comment": "Rule (check) display name."}),
        T.StructField("message", T.StringType(), True, {"comment": "Human-friendly message from DQX."}),
        T.StructField("columns", T.ArrayType(T.StringType()), True, {"comment": "Columns referenced by the rule."}),
        T.StructField("filter", T.StringType(), True, {"comment": "Predicate applied prior to rule evaluation (if any)."}),
        T.StructField("function", T.StringType(), True, {"comment": "DQX function that produced the issue."}),
        T.StructField("run_time", T.TimestampType(), True, {"comment": "Per-issue timestamp (if populated by DQX)."}),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True, {"comment": "Free-form metadata map from rule definition."}),
        T.StructField("check_id", T.StringType(), True, {"comment": "Originating check_id resolved from checks_config."}),
    ])), False, {"comment": "Array of error issues as emitted by DQX, enriched with check_id."}),
    T.StructField("_errors_fingerprint", T.StringType(), False, {
        "comment": "Deterministic digest of normalized _errors (order-insensitive)."
    }),

    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name", T.StringType(), True, {"comment": "Rule (check) display name."}),
        T.StructField("message", T.StringType(), True, {"comment": "Human-friendly message from DQX."}),
        T.StructField("columns", T.ArrayType(T.StringType()), True, {"comment": "Columns referenced by the rule."}),
        T.StructField("filter", T.StringType(), True, {"comment": "Predicate applied prior to rule evaluation (if any)."}),
        T.StructField("function", T.StringType(), True, {"comment": "DQX function that produced the issue."}),
        T.StructField("run_time", T.TimestampType(), True, {"comment": "Per-issue timestamp (if populated by DQX)."}),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True, {"comment": "Free-form metadata map from rule definition."}),
        T.StructField("check_id", T.StringType(), True, {"comment": "Originating check_id resolved from checks_config."}),
    ])), False, {"comment": "Array of warning issues as emitted by DQX, enriched with check_id."}),
    T.StructField("_warnings_fingerprint", T.StringType(), False, {
        "comment": "Deterministic digest of normalized _warnings (order-insensitive)."
    }),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column", T.StringType(), False, {"comment": "Column name."}),
        T.StructField("value", T.StringType(), True, {"comment": "Stringified value at evaluation time."}),
    ])), False, {"comment": "Snapshot of non-reserved columns for the flagged row."}),
    T.StructField("row_snapshot_fingerprint", T.StringType(), False, {
        "comment": "sha256(JSON(row_snapshot)) used in log_id and de-duplication."
    }),

    T.StructField("created_by", T.StringType(), False, {"comment": "Audit: writer identity."}),
    T.StructField("created_at", T.TimestampType(), False, {"comment": "Audit: creation timestamp (UTC)."}),
    T.StructField("updated_by", T.StringType(), True, {"comment": "Audit: last updater (nullable)."}),
    T.StructField("updated_at", T.TimestampType(), True, {"comment": "Audit: update timestamp (UTC, nullable)."}),
])

CHECKS_LOG_SUMMARY_BY_RULE_STRUCT = T.StructType([
    T.StructField("run_config_name", T.StringType(), False, {"comment": "Run configuration name."}),
    T.StructField("table_name", T.StringType(), False, {"comment": "Source table name."}),
    T.StructField("rule_name", T.StringType(), False, {"comment": "Rule (check) name being summarized."}),
    T.StructField("severity", T.StringType(), False, {"comment": "Normalized severity: error | warning."}),
    T.StructField("rows_flagged", T.LongType(), False, {"comment": "Total rows flagged by this rule within the table."}),
    T.StructField("table_total_rows", T.LongType(), True, {"comment": "Total rows scanned in the table (nullable if unknown)."}),
    T.StructField("pct_of_table_rows", T.DoubleType(), False, {"comment": "rows_flagged / table_total_rows (0 if unknown)."}),
])

CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT = T.StructType([
    T.StructField("run_config_name", T.StringType(), False, {"comment": "Run configuration name."}),
    T.StructField("table_name", T.StringType(), False, {"comment": "Source table name."}),
    T.StructField("table_total_rows", T.LongType(), False, {"comment": "Total rows scanned."}),
    T.StructField("table_total_error_rows", T.LongType(), False, {"comment": "Rows with at least one error."}),
    T.StructField("table_total_warning_rows", T.LongType(), False, {"comment": "Rows with at least one warning."}),
    T.StructField("total_flagged_rows", T.LongType(), False, {"comment": "Rows with any error or warning."}),
    T.StructField("distinct_rules_fired", T.IntegerType(), False, {"comment": "Distinct rule names that fired in the table."}),
])


# =====================================================================================
# Issue shapes / helpers
# =====================================================================================
DQX_ISSUE_STRUCT = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("message", T.StringType(), True),
    T.StructField("columns", T.ArrayType(T.StringType()), True),
    T.StructField("filter", T.StringType(), True),
    T.StructField("function", T.StringType(), True),
    T.StructField("run_time", T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
])
DQX_ISSUE_ARRAY = T.ArrayType(DQX_ISSUE_STRUCT)

ISSUE_WITH_ID_STRUCT = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("message", T.StringType(), True),
    T.StructField("columns", T.ArrayType(T.StringType()), True),
    T.StructField("filter", T.StringType(), True),
    T.StructField("function", T.StringType(), True),
    T.StructField("run_time", T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("check_id", T.StringType(), True),
])
ISSUE_WITH_ID_ARRAY = T.ArrayType(ISSUE_WITH_ID_STRUCT)

def _empty_raw_issues() -> F.Column:
    return F.from_json(F.lit("[]"), DQX_ISSUE_ARRAY)

def _empty_with_id_issues() -> F.Column:
    return F.from_json(F.lit("[]"), ISSUE_WITH_ID_ARRAY)

def _normalize_to_raw_issue_array(col: F.Column) -> F.Column:
    js = F.when(col.isNull(), F.lit("[]")).otherwise(F.to_json(col))
    return F.coalesce(F.from_json(js, DQX_ISSUE_ARRAY), _empty_raw_issues())


# =====================================================================================
# JIT argument coercion (same as your working notebook)
# =====================================================================================
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


# =====================================================================================
# Load checks from table (as dicts, not Rows → avoid .get() on Row)
# =====================================================================================
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


# =====================================================================================
# Apply checks (isolate bad rules if batch fails)
# =====================================================================================
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
                    print(f"    offending rule JSON:\n{json.dumps(r, indent=2)}")
                except Exception:
                    pass

        if not good:
            return None, bad

        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception as ex2:
            print(f"[{table_name}] still failing after pruning bad rules: {ex2}")
            return None, bad


# =====================================================================================
# Row projection (RAW arrays from DQX)
# =====================================================================================
def _pick_col(df: DataFrame, *cands: str) -> Optional[str]:
    for c in cands:
        if c in df.columns:
            return c
    return None

def _normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # normalize for fingerprint (order-insensitive, sort 'columns'; drop run_time/user_metadata/check_id)
    return F.transform(
        arr_col,
        lambda r: F.struct(
            F.lower(F.trim(r["name"])).alias("name"),
            F.trim(r["message"]).alias("message"),
            F.array_sort(F.transform(F.coalesce(r["columns"], F.array().cast(T.ArrayType(T.StringType()))),
                                     lambda c: F.lower(F.trim(c)))).alias("columns"),
            F.lower(F.trim(r["filter"])).alias("filter"),
            F.lower(F.trim(r["function"])).alias("function"),
        ),
    )

def _project_row_hits(df_annot: DataFrame,
                      table_name: str,
                      run_config_name: str,
                      created_by: str,
                      exclude_cols: Optional[List[str]] = None) -> DataFrame:
    exclude_cols = set(exclude_cols or [])
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errs = _normalize_to_raw_issue_array(F.col(e_name) if e_name else F.lit(None))
    wrns = _normalize_to_raw_issue_array(F.col(w_name) if w_name else F.lit(None))

    df = (df_annot
          .withColumn("_errs", errs)
          .withColumn("_warns", wrns)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]

    row_snapshot = F.array(*[
        F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value"))
        for c in sorted(cols)
    ])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_warns")))), 256)

    log_id = F.sha2(
        F.concat_ws("||", F.lit(table_name), F.lit(run_config_name), row_snapshot_fp, _errors_fp, _warnings_fp),
        256
    )

    return df.select(
        log_id.alias("log_id"),
        F.lit(table_name).alias("table_name"),
        F.lit(run_config_name).alias("run_config_name"),
        F.col("_errs").alias("_errors"),     # RAW arrays here (no check_id yet)
        _errors_fp.alias("_errors_fingerprint"),
        F.col("_warns").alias("_warnings"),  # RAW arrays here (no check_id yet)
        _warnings_fp.alias("_warnings_fingerprint"),
        row_snapshot.alias("row_snapshot"),
        row_snapshot_fp.alias("row_snapshot_fingerprint"),
        F.lit(created_by).alias("created_by"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast(T.StringType()).alias("updated_by"),
        F.lit(None).cast(T.TimestampType()).alias("updated_at"),
    )


# =====================================================================================
# Embed check_id per issue (convert to WITH-ID arrays; JSON trick avoids struct ordering error)
# =====================================================================================
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

    issue_struct_type = ISSUE_WITH_ID_STRUCT  # reuse exact target type

    def enrich(colname: str) -> DataFrame:
        log_side = (
            row_log_df
            .select("log_id", "table_name", "run_config_name",
                    F.posexplode_outer(F.col(colname)).alias("pos", "iss"))
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
            .agg(F.first("iss", ignorenulls=True).alias("iss"),
                 F.max("cfg_check_id").alias("issue_check_id"))
        )

        # Build JSON so array_sort compares orderable scalars, not nested structs
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
        row_log_df.drop("_errors","_warnings")
        .join(err_arr,  "log_id", "left")
        .join(warn_arr, "log_id", "left")
        .withColumn("_errors",   F.coalesce(F.col("_errors"),   _empty_with_id_issues()))
        .withColumn("_warnings", F.coalesce(F.col("_warnings"), _empty_with_id_issues()))
    )


# =====================================================================================
# Summaries
# =====================================================================================
def _summarize_table(annot: DataFrame, table_name: str) -> Row:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    annot = annot.cache()
    tot = annot.count()
    sums = (annot
        .select(
            (F.size(F.col(err)) > 0).cast("int").alias("e"),
            (F.size(F.col(wrn)) > 0).cast("int").alias("w"),
            ((F.size(F.col(err)) > 0) | (F.size(F.col(wrn)) > 0)).cast("int").alias("f"),
        )
        .agg(F.sum("e").alias("e"), F.sum("w").alias("w"), F.sum("f").alias("f"))
        .collect()[0]
    )
    names_arr = F.expr(f"array_union(transform({err}, x -> x.name), transform({wrn}, x -> x.name))")
    rules = (annot
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


# =====================================================================================
# Runner
# =====================================================================================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "strict",
) -> Dict[str, Any]:

    nb = cfg.notebook(notebook_idx)

    checks_table = (
        f'{cfg.get("notebooks.notebook_1.targets.target_table_1.catalog")}.'
        f'{cfg.get("notebooks.notebook_1.targets.target_table_1.schema")}.'
        f'{cfg.get("notebooks.notebook_1.targets.target_table_1.table")}'
    )

    targets = nb.targets()
    t1 = targets.target_table(1)
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    tw = TableWriter(spark)

    # Row log table
    results_fqn   = t1.full_table_name()
    results_write = t1.get("write") or {}
    results_pk    = t1.get("primary_key")
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_tags  = t1.table_tags()

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
            # NOTE: not passing column_comments → same style as Notebook 1 (comments live in schema)
        )

    # Summary by rule table (optional)
    if t2:
        s2_fqn    = t2.full_table_name()
        t2_write  = t2.get("write") or {}
        s2_desc   = t2.get("table_description")
        s2_tags   = t2.table_tags()
        s2_part   = t2.get("partition_by") or []
        if not table_exists(spark, s2_fqn):
            tw.create_table(
                fqn=s2_fqn,
                schema=CHECKS_LOG_SUMMARY_BY_RULE_STRUCT,
                format=t2_write.get("format"),
                options=t2_write.get("options") or {},
                partition_by=s2_part,
                table_comment=s2_desc,
                table_tags=s2_tags,
                primary_key_cols=[t2.get("primary_key")] if t2.get("primary_key") else None,
            )

    # Summary by table (optional)
    if t3:
        s3_fqn    = t3.full_table_name()
        t3_write  = t3.get("write") or {}
        s3_desc   = t3.get("table_description")
        s3_tags   = t3.table_tags()
        s3_part   = t3.get("partition_by") or []
        if not table_exists(spark, s3_fqn):
            tw.create_table(
                fqn=s3_fqn,
                schema=CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT,
                format=t3_write.get("format"),
                options=t3_write.get("options") or {},
                partition_by=s3_part,
                table_comment=s3_desc,
                table_tags=s3_tags,
                primary_key_cols=[t3.get("primary_key")] if t3.get("primary_key") else None,
            )

    dq = DQEngine(WorkspaceClient())

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []

    # Discover run_config_names from checks table (active only)
    rc_names = [r["run_config_name"] for r in (
        spark.table(checks_table)
        .where(F.col("active") == True)
        .select("run_config_name").dropDuplicates().collect()
    ) if r["run_config_name"]]

    for rc_name in sorted(rc_names):
        display_section(f"Run config: {rc_name}")

        # Load rules (as dicts), JIT coerce args, validate with DQX
        by_tbl, coerced, skipped = _load_checks_from_table_as_dicts(
            spark, checks_table, rc_name, coercion_mode=coercion_mode
        )
        checks_loaded = sum(len(v) for v in by_tbl.values())
        print(f"[{rc_name}] checks_in_table_total={checks_table_total}, loaded={checks_loaded}, coerced={coerced}, skipped_invalid={skipped}")

        if not checks_loaded:
            print(f"[{rc_name}] no checks loaded.")
            continue

        out_batches: List[DataFrame] = []
        rc_tbl_summaries: List[Row] = []
        rc_rule_hit_parts: List[DataFrame] = []
        table_row_counts: Dict[str, int] = {}
        processed_tables: List[str] = []

        for tbl, tbl_rules in sorted(by_tbl.items()):
            try:
                src = spark.read.table(tbl)
            except Exception as e:
                print(f"[{rc_name}] {tbl} read failed: {e}")
                continue

            annot, bad = _apply_rules_isolating_failures(dq, src, tbl, tbl_rules)
            if annot is None:
                if bad:
                    bad_df = spark.createDataFrame(
                        [Row(run_config_name=rc_name,
                             table_name=tbl,
                             rule_name=nm,
                             severity="unknown",
                             yaml_path="",
                             reason=msg) for nm, msg in bad],
                        schema="run_config_name string, table_name string, rule_name string, severity string, yaml_path string, reason string",
                    )
                    display_section(f"Skipped rules (apply errors) — run_config={rc_name}, table={tbl}")
                    show_df(bad_df.orderBy("rule_name"), n=1000, truncate=False)
                continue

            processed_tables.append(tbl)
            total_rows = annot.count()
            table_row_counts[tbl] = total_rows

            summary_row = _summarize_table(annot, tbl)
            rc_tbl_summaries.append(summary_row)
            all_tbl_summaries.append(Row(run_config_name=rc_name, **summary_row.asDict()))
            rc_rule_hit_parts.append(_rules_hits_for_table(annot, tbl))

            row_hits = _project_row_hits(annot, tbl, rc_name, created_by="AdminUser", exclude_cols=exclude_cols)
            if row_hits.limit(1).count() > 0:
                out_batches.append(row_hits)

        if rc_tbl_summaries:
            summary_df = spark.createDataFrame(rc_tbl_summaries).orderBy("table_name")
            display_section(f"Row-hit summary by table (run_config={rc_name})")
            show_df(summary_df, n=200, truncate=False)

        # Summary by rule (optional)
        if rc_rule_hit_parts and t2:
            summary_by_rule_fqn = t2.full_table_name()
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

            t2_write = t2.get("write") or {}
            (full_rules
             .withColumn("run_config_name", F.lit(rc_name))
             .select("run_config_name","table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
             .write
             .format(t2_write.get("format"))
             .mode(t2_write.get("mode"))
             .options(**(t2_write.get("options") or {}))
             .saveAsTable(summary_by_rule_fqn))

        # Row-level writes
        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), out_batches)

        # Enrich check_id → WITH-ID arrays
        out = _embed_issue_check_ids(out, checks_table)

        # Enforce PK idempotency
        pk_cols = [t1.get("primary_key")] if isinstance(t1.get("primary_key"), str) else list(t1.get("primary_key"))
        dupes = out.groupBy(*pk_cols).count().where(F.col("count") > 1)
        if dupes.limit(1).count() > 0:
            raise RuntimeError(f"results batch contains duplicate PKs: {pk_cols}")
        out = out.dropDuplicates(pk_cols)

        tw.write_df(
            df=out.select(*[f.name for f in CHECKS_LOG_STRUCT.fields]),
            fqn=results_fqn,
            mode=results_write.get("mode"),
            format=results_write.get("format"),
            options=results_write.get("options") or {},
        )

        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # Summary by table (grand)
        if t3:
            summary_by_table_fqn = t3.full_table_name()
            grand_df = (
                spark.createDataFrame(all_tbl_summaries)
                .select("run_config_name","table_name","table_total_rows","table_total_error_rows",
                        "table_total_warning_rows","total_flagged_rows","distinct_rules_fired")
                .orderBy("run_config_name","table_name")
            )
            target_schema = spark.table(summary_by_table_fqn).schema
            grand_df_aligned = grand_df.select(
                *[F.col(f.name).cast(f.dataType).alias(f.name) for f in target_schema.fields]
            )
            t3_write = t3.get("write") or {}
            (grand_df_aligned
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
    result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2)
    print(result)

# COMMAND ----------

# DBTITLE 1,Validation
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RowCountExample") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the table
df = spark.table("dq_dev.dqx.checks_log")

# Get row count
row_count = df.count()

print(f"Row count: {row_count}")
