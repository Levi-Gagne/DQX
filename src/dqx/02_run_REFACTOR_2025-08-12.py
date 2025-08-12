# Databricks notebook: 02_run_dqx_checks (DISPLAY-FIRST, CLEAN SUMMARIES, TABLE DOC DICT)
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

import json
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

# ======================================================
# DISPLAY HELPERS (use Databricks display() when present)
# ======================================================

def _can_display() -> bool:
    return "display" in globals()

def _section(title: str) -> None:
    print("\n" + "═" * 80)
    print(f"║ {title}")
    print("═" * 80)

def show_df(df: DataFrame, n: int = 200, truncate: bool = False) -> None:
    if _can_display():
        display(df.limit(n))
    else:
        df.show(n, truncate=truncate)

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

# --------------------------------------------------------------------
# TABLE DOCUMENTATION (markdown strings) — PASS THIS DICT TO ensure_table
# --------------------------------------------------------------------
RESULTS_TABLE_DOC: Dict[str, Any] = {
    "table": "<AUTO>",  # will be replaced with actual FQN when applied
    "table_comment": (
        "# DQX Row-Level Findings Log\n"
        "- Captures **row-level hits** from DQX checks (errors & warnings) per run configuration.\n"
        "- `log_id` is a deterministic hash of table, run-config, row snapshot & issue fingerprints.\n"
        "- `check_id` array backfills IDs for rules that hit the row (joined by rule name).\n"
        "- Fingerprints (`*_fingerprint`) are **stable** hashes to group identical results across runs.\n"
    ),
    "columns": {
        "log_id": "Deterministic **hash**: table_name || run_config_name || row_snapshot_fingerprint || issues.",
        "check_id": "Array of **check_id** values that hit this row (resolved by rule name).",
        "table_name": "Fully qualified **source table** (`catalog.schema.table`).",
        "run_config_name": "DQX **run group/tag** used to select rules.",
        "_errors": "Array of **error** hits (struct: name, message, columns, filter, function, run_time, user_metadata).",
        "_errors_fingerprint": "Stable **hash** of normalized `_errors` array.",
        "_warnings": "Array of **warning** hits (same struct as `_errors`).",
        "_warnings_fingerprint": "Stable **hash** of normalized `_warnings` array.",
        "row_snapshot": "Array<struct{column,value}> of the row (non-reserved columns only), cast to string.",
        "row_snapshot_fingerprint": "Stable **hash** of `row_snapshot`.",
        "created_by": "Audit: **creator** of the row.",
        "created_at": "Audit: **creation timestamp**.",
        "updated_by": "Audit: **last updater**.",
        "updated_at": "Audit: **last update timestamp**.",
    },
}

# --------------------------
# COMMENT / DOC APPLICATION
# --------------------------
def _esc_comment(s: str) -> str:
    return (s or "").replace("'", "''")

def _q_fqn(fqn: str) -> str:
    return ".".join(f"`{p}`" for p in fqn.split("."))

def _apply_table_doc(spark: SparkSession, full_name: str, doc: Optional[Dict[str, Any]], created_now: bool) -> None:
    if not doc:
        return
    q = _q_fqn(full_name)
    table_comment = _esc_comment(doc.get("table_comment", ""))
    if table_comment:
        try:
            spark.sql(f"COMMENT ON TABLE {q} IS '{table_comment}'")
        except Exception:
            spark.sql(f"ALTER TABLE {q} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    if not created_now:
        return  # per requirement: add column comments only on first create

    existing = {f.name.lower() for f in spark.table(full_name).schema.fields}
    for col, comment in (doc.get("columns") or {}).items():
        if col.lower() not in existing:
            continue
        try:
            spark.sql(f"ALTER TABLE {q} ALTER COLUMN `{col}` COMMENT '{_esc_comment(comment)}'")
        except Exception as e:
            print(f"[meta] Skipped column comment for {full_name}.{col}: {e}")

def _preview_table_doc(spark: SparkSession, full_name: str, doc: Dict[str, Any]) -> None:
    _section("TABLE METADATA PREVIEW (markdown text stored in comments)")
    show_df(spark.createDataFrame([(full_name, doc.get("table_comment", ""))],
                                  "table string, table_comment_markdown string"), n=1, truncate=False)
    cols = doc.get("columns") or {}
    show_df(spark.createDataFrame([(k, v) for k, v in cols.items()],
                                  "column string, column_comment_markdown string"), n=200, truncate=False)

# --------------------------
# Ensure results table exists
# --------------------------
def ensure_table(full_name: str, table_doc: Optional[Dict[str, Any]] = None):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    existed = spark.catalog.tableExists(full_name)
    if not existed:
        cat, sch, _ = full_name.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        spark.createDataFrame([], ROW_LOG_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(full_name)
        # apply comments (table always; columns only on first create)
        doc = dict(table_doc or {})
        if doc.get("table") in (None, "", "<AUTO>"):
            doc["table"] = full_name
        _apply_table_doc(spark, full_name, doc, created_now=True)
        _preview_table_doc(spark, full_name, doc)
    else:
        # Update only table comment on subsequent runs
        if table_doc:
            doc = dict(table_doc)
            if doc.get("table") in (None, "", "<AUTO>"):
                doc["table"] = full_name
            _apply_table_doc(spark, full_name, doc, created_now=False)

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
# JIT argument coercion
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
# Load checks from config table
# --------------------------
def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for r in rules:
        out.setdefault(r["table_name"], []).append(r)
    return out

def _load_checks_from_table_as_dicts(spark: SparkSession,
                                     checks_table: str,
                                     run_config_name: str,
                                     coercion_mode: str = "permissive") -> Tuple[Dict[str, List[dict]], int, int, List[dict]]:
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

    bad_dump: List[dict] = []
    status = DQEngine.validate_checks(raw_rules)
    if getattr(status, "has_errors", False):
        keep: List[dict] = []
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if getattr(st, "has_errors", False):
                bad_dump.append(r)
            else:
                keep.append(r)
        return _group_by_table(keep), coerced, len(bad_dump), bad_dump
    else:
        return _group_by_table(raw_rules), coerced, 0, bad_dump

# --------------------------
# Apply with isolation & diagnostics
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
        if not good:
            return None, bad
        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception as ex2:
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
        F.lit(None).cast(T.ArrayType(T.StringType())).alias("check_id"),
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
# Summaries (DISPLAY-FIRST)
# --------------------------
def _summarize_hits(df_annot: DataFrame) -> DataFrame:
    """Return a single-row DF: errors, warnings, total_issues — non-negative by construction."""
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")
    errs = F.coalesce(F.sum(F.size(F.col(e_name))) if e_name else F.lit(0).cast("long"), F.lit(0).cast("long"))
    warns = F.coalesce(F.sum(F.size(F.col(w_name))) if w_name else F.lit(0).cast("long"), F.lit(0).cast("long"))
    agg = df_annot.agg(errs.alias("errors"), warns.alias("warnings"))
    return agg.select(
        F.col("errors"),
        F.col("warnings"),
        (F.col("errors") + F.col("warnings")).alias("total_issues"),
    )

def _top_rule_counts(df_annot: DataFrame, limit:int = 20) -> DataFrame:
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")
    errs = F.explode_outer(F.col(e_name)) if e_name else None
    warns = F.explode_outer(F.col(w_name)) if w_name else None
    pieces = []
    if errs is not None:
        pieces.append(df_annot.select(errs.alias("h")).select(F.col("h.name").alias("name"), F.lit("error").alias("severity")))
    if warns is not None:
        pieces.append(df_annot.select(warns.alias("h")).select(F.col("h.name").alias("name"), F.lit("warning").alias("severity")))
    if not pieces:
        return df_annot.sql_ctx.createDataFrame([], "name string, severity string, count long")
    u = pieces[0]
    for p in pieces[1:]:
        u = u.unionByName(p, allowMissingColumns=True)
    return (
        u.where(F.col("name").isNotNull())
         .groupBy("name", "severity")
         .count()
         .orderBy(F.desc("count"), F.asc("name"))
         .limit(limit)
    )

# ---------------
# Main entry point
# ---------------
def run_checks(
    dqx_cfg_yaml: str,
    created_by: str = "AdminUser",
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive",  # or "strict"
    table_doc: Optional[Dict[str, Any]] = RESULTS_TABLE_DOC,
):
    spark = SparkSession.builder.getOrCreate()
    dq = DQEngine(WorkspaceClient())

    cfg = read_yaml(dqx_cfg_yaml)
    checks_table  = cfg["dqx_checks_config_table_name"]   # e.g., dq_dev.dqx.checks_config
    results_table = cfg["dqx_checks_log_table_name"]      # e.g., dq_dev.dqx.checks_log
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}

    # Ensure results table exists + comments
    ensure_table(results_table, table_doc=table_doc)

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    rc_summary_rows: List[Tuple[str, int, int, int, int]] = []
    grand_total = 0

    for rc_name, rc_cfg in rc_map.items():
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        write_mode = (rc_cfg or {}).get("output_config", {}).get("mode", "overwrite")
        write_opts = (rc_cfg or {}).get("output_config", {}).get("options", {}) or {}

        by_tbl, coerced, skipped, bad_rules = _load_checks_from_table_as_dicts(
            spark, checks_table, rc_name, coercion_mode=coercion_mode
        )
        checks_loaded = sum(len(v) for v in by_tbl.values())

        # Run-config header summary
        _section(f"RUN CONFIG: {rc_name}")
        show_df(
            spark.createDataFrame(
                [(rc_name, checks_table_total, checks_loaded, coerced, skipped)],
                schema="run_config string, checks_in_table_total long, loaded long, coerced long, skipped_invalid long",
            ),
            n=1,
        )

        if bad_rules:
            _section(f"INVALID RULES (skipped) — {rc_name}")
            show_df(spark.createDataFrame(
                [(r.get("table_name"), r.get("name"), json.dumps(r.get("check"), ensure_ascii=False)) for r in bad_rules],
                "table_name string, rule_name string, check_json string"
            ), n=200, truncate=False)

        if not checks_loaded:
            print(f"[{rc_name}] no checks loaded (active=TRUE).")
            rc_summary_rows.append((rc_name, 0, 0, 0, 0))
            continue

        out_batches: List[DataFrame] = []
        per_table_rows: List[Tuple[str, int, int, int]] = []

        for tbl, tbl_rules in by_tbl.items():
            # source access
            try:
                src = spark.read.table(tbl)
            except Exception as e:
                print(f"[{rc_name}] {tbl} failed to read: {e}")
                continue

            # apply rules (isolate failures)
            annot, bad = _apply_rules_isolating_failures(dq, src, tbl, tbl_rules)
            if annot is None:
                continue

            # per-table summary (display)
            _section(f"SUMMARY OF HITS — {tbl}")
            hits = _summarize_hits(annot)
            show_df(hits, n=1)
            top = _top_rule_counts(annot, limit=20)
            _section(f"TOP RULE HITS — {tbl}")
            show_df(top, n=20)

            # project row-level hits
            row_hits = _project_row_hits(annot, tbl, rc_name, created_by, exclude_cols=exclude_cols)
            if row_hits.limit(1).count() > 0:
                out_batches.append(row_hits)

            # cache counts for RC summary
            r = hits.collect()[0]
            per_table_rows.append((tbl, int(r["errors"]), int(r["warnings"]), int(r["total_issues"])))

        if per_table_rows:
            _section(f"RULES LOADED PER TABLE — {rc_name}")
            per_tbl_df = spark.createDataFrame(per_table_rows, "table_name string, errors long, warnings long, total_issues long") \
                               .orderBy(F.desc("total_issues"))
            show_df(per_tbl_df, n=200)

        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            rc_summary_rows.append((rc_name, checks_loaded, coerced, skipped, 0))
            continue

        # union batches and enrich check_id
        out = out_batches[0]
        for b in out_batches[1:]:
            out = out.unionByName(b, allowMissingColumns=True)
        out = _enrich_check_ids(out, checks_table)

        # write
        out = out.select([f.name for f in ROW_LOG_SCHEMA.fields])
        rows = out.count()
        out.write.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
        grand_total += rows

        _section("WRITE RESULT (Delta)")
        show_df(spark.createDataFrame([(rows, results_table)], "`rows written` long, `target table` string"), n=1)

        rc_summary_rows.append((rc_name, checks_loaded, coerced, skipped, rows))

    # RC-level summary
    _section("RUN CONFIG SUMMARY")
    show_df(
        spark.createDataFrame(
            rc_summary_rows,
            "run_config string, loaded long, coerced long, skipped_invalid long, rows_written long",
        ),
        n=200,
    )

    print(f"TOTAL rows written: {grand_total}")


# ---- run it ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"],
    coercion_mode="strict",      # 'strict' | 'permissive'
    table_doc=RESULTS_TABLE_DOC  # pass the dict defined above
)