# Databricks notebook source
# MAGIC %md
# MAGIC # Run DQX Checks

# COMMAND ----------

# === Cell 1: Install DQX Package (if your cluster image doesn't already include it) ===
%pip install databricks-labs-dqx==0.8.0

# COMMAND ----------

# === Cell 2: Restart Python to pick up libs (Databricks convention) ===
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
