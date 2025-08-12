# Databricks notebook: 02_run_dqx_checks
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# -------------------
# Config / IO helpers
# -------------------
def read_yaml(path: str) -> Dict[str, Any]:
    p = path.replace("dbfs:/", "/dbfs/") if path.startswith("dbfs:/") else path
    with open(p, "r") as fh:
        return yaml.safe_load(fh) or {}


# -------------
# Result schema
# -------------
# One row per offending source row. Keep arrays intact. Fingerprints adjacent to arrays.
ROW_LOG_SCHEMA = T.StructType([
    T.StructField("result_id",        T.StringType(),  False),  # PK (deterministic)
    T.StructField("table_name",       T.StringType(),  False),
    T.StructField("run_config_name",  T.StringType(),  False),

    # Issues (arrays of structs)
    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_errors_fingerprint",   T.StringType(), False),

    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    ])), False),
    T.StructField("_warnings_fingerprint", T.StringType(), False),

    # Snapshot of source row (KV array) + fingerprint
    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column", T.StringType(), False),
        T.StructField("value",  T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint", T.StringType(), False),

    # Backrefs to checks table PKs (hash_id) for all issues on the row
    T.StructField("check_ids", T.ArrayType(T.StringType()), True),

    # Audit
    T.StructField("created_by",  T.StringType(),  False),
    T.StructField("created_at",  T.TimestampType(), False),
    T.StructField("updated_by",  T.StringType(),  True),
    T.StructField("updated_at",  T.TimestampType(), True),
])


def ensure_table(full_name: str):
    spark = SparkSession.getActiveSession()
    if not spark.catalog.tableExists(full_name):
        cat, sch, _ = full_name.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        spark.createDataFrame([], ROW_LOG_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(full_name)


# --------------------------
# Internals / transformations
# --------------------------
def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _empty_issues_array():
    # robust empty array<struct> matching the issues struct above
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
    # For stable hashing: sort 'columns'; drop volatile fields from the fingerprint
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


def _project_row_hits(df_annot: DataFrame,
                      table_name: str,
                      run_config_name: str,
                      created_by: str,
                      exclude_cols: Optional[List[str]] = None) -> DataFrame:
    """
    From an annotated DF (has _error/_errors/_warning/_warnings), return only rows with hits.
    Keep _errors and _warnings arrays intact, compute adjacent fingerprints, row snapshot + fp, and result_id.
    """
    exclude_cols = set(exclude_cols or [])

    # Handle DQX versioned names
    e_name = _pick_col(df_annot, "_errors", "_error")
    w_name = _pick_col(df_annot, "_warnings", "_warning")

    errors_col   = F.col(e_name) if e_name else _empty_issues_array()
    warnings_col = F.col(w_name) if w_name else _empty_issues_array()

    df = (df_annot
          .withColumn("_errs", errors_col)
          .withColumn("_warns", warnings_col)
          .where((F.size("_errs") > 0) | (F.size("_warns") > 0)))

    # Build row snapshot: all columns except the temp DQX arrays and optional excludes
    reserved = {e_name, w_name, "_errs", "_warns"} - {None} | exclude_cols
    cols = [c for c in df.columns if c not in reserved]
    row_snapshot = F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value")) for c in sorted(cols)])
    row_snapshot_fp = F.sha2(F.to_json(row_snapshot), 256)

    _errors_fp   = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_errs")))), 256)
    _warnings_fp = F.sha2(F.to_json(F.array_sort(_normalize_issues_for_fp(F.col("_warns")))), 256)

    result_id = F.sha2(F.concat_ws("||",
                                   F.lit(table_name),
                                   F.lit(run_config_name),
                                   row_snapshot_fp,
                                   _errors_fp,
                                   _warnings_fp), 256)

    out = df.select(
        result_id.alias("result_id"),
        F.lit(table_name).alias("table_name"),
        F.lit(run_config_name).alias("run_config_name"),
        F.col("_errs").alias("_errors"),
        _errors_fp.alias("_errors_fingerprint"),
        F.col("_warns").alias("_warnings"),
        _warnings_fp.alias("_warnings_fingerprint"),
        row_snapshot.alias("row_snapshot"),
        row_snapshot_fp.alias("row_snapshot_fingerprint"),
        F.lit(None).cast(T.ArrayType(T.StringType())).alias("check_ids"),
        F.lit(created_by).alias("created_by"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast(T.StringType()).alias("updated_by"),
        F.lit(None).cast(T.TimestampType()).alias("updated_at"),
    )
    return out


def _enrich_check_ids(row_log_df: DataFrame, checks_table: str) -> DataFrame:
    """
    For each row_log issue name (errors + warnings), join to checks_config to fetch hash_id.
    Produces check_ids: array<string> (distinct).
    """
    spark = row_log_df.sql_ctx.sparkSession
    chk = spark.table(checks_table).select(
        F.col("table_name").alias("t_tbl"),
        F.col("run_config_name").alias("t_rc"),
        F.col("name").alias("t_name"),
        F.col("hash_id").alias("check_id"),
        F.col("active").alias("t_active")
    )

    names = (
        row_log_df
        .select("result_id", "table_name", "run_config_name",
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
        .groupBy("result_id")
        .agg(F.array_sort(F.array_distinct(F.collect_list("check_id"))).alias("check_ids"))
    )

    return (
        row_log_df.drop("check_ids")
        .join(j, "result_id", "left")
        .withColumn("check_ids", F.expr("filter(check_ids, x -> x is not null)"))
    )


def _print_summary_for_rc(rc_name: str,
                          checks_loaded: int,
                          checks_table_total: int,
                          out_df: Optional[DataFrame]):
    print(f"[{rc_name}] checks_in_table_total={checks_table_total}, checks_loaded_for_rc={checks_loaded}")
    if out_df is None or out_df.rdd.isEmpty():
        print(f"[{rc_name}] no hits.")
        return

    # Per-check hit counts (errors + warnings as “hit”)
    hits = (
        out_df
        .select(
            F.col("table_name"),
            F.explode_outer(
                F.array_union(
                    F.expr("transform(_errors, x -> struct(x.name as name, 'error' as severity))"),
                    F.expr("transform(_warnings, x -> struct(x.name as name, 'warning' as severity))"),
                )
            ).alias("h")
        )
        .select("table_name", "h.name", "h.severity")
        .groupBy("table_name", "name")
        .count()
        .orderBy(F.desc("count"), F.asc("table_name"), F.asc("name"))
    )

    print(f"[{rc_name}] top check hits:")
    hits.show(50, truncate=False)


# ---------------
# Main entry point
# ---------------
def run_checks(
    dqx_cfg_yaml: str,
    created_by: str = "AdminUser",
    exclude_cols: Optional[List[str]] = None
):
    spark = SparkSession.builder.getOrCreate()
    dq = DQEngine(WorkspaceClient())

    cfg = read_yaml(dqx_cfg_yaml)
    checks_table  = cfg["dqx_checks_config_table_name"]   # e.g., dq_dev.dqx.checks_config
    results_table = cfg["dqx_checks_log_table_name"]      # e.g., dq_dev.dqx.checks_log
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}

    ensure_table(results_table)

    # global count for context
    try:
        checks_table_total = spark.table(checks_table).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    for rc_name, rc_cfg in rc_map.items():
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        write_mode = (rc_cfg or {}).get("output_config", {}).get("mode", "overwrite")
        write_opts = (rc_cfg or {}).get("output_config", {}).get("options", {}) or {}

        # Load checks from table for this run-config
        checks = dq.load_checks(TableChecksStorageConfig(location=checks_table, run_config_name=rc_name))
        checks_loaded = len(checks or [])
        if not checks_loaded:
            _print_summary_for_rc(rc_name, 0, checks_table_total, None)
            continue

        # DQX built-in validation on table-sourced rules
        status = DQEngine.validate_checks(checks)
        if getattr(status, "has_errors", False):
            print(f"[{rc_name}] invalid checks:\n{status.to_string()}")
            continue

        # Group by table and run once per table (single scan)
        by_tbl: Dict[str, List[dict]] = {}
        for c in checks:
            t = c.get("table_name") or c.get("table")
            if t:
                by_tbl.setdefault(t, []).append(c)

        out_batches: List[DataFrame] = []
        for tbl, tbl_checks in by_tbl.items():
            try:
                src = spark.read.table(tbl)
                annot = dq.apply_checks_by_metadata(src, tbl_checks)
            except Exception as e:
                print(f"[{rc_name}] {tbl} failed: {e}")
                continue

            row_hits = _project_row_hits(annot, tbl, rc_name, created_by, exclude_cols=exclude_cols)
            if not row_hits.rdd.isEmpty():
                out_batches.append(row_hits)

        if not out_batches:
            _print_summary_for_rc(rc_name, checks_loaded, checks_table_total, None)
            continue

        out = out_batches[0]
        for b in out_batches[1:]:
            out = out.unionByName(b, allowMissingColumns=True)

        out = _enrich_check_ids(out, checks_table)

        # enforce schema/order, write, print summary
        out = out.select([f.name for f in ROW_LOG_SCHEMA.fields])
        rows = out.count()
        out.write.format("delta").mode(write_mode).options(**write_opts).saveAsTable(results_table)
        grand_total += rows

        _print_summary_for_rc(rc_name, checks_loaded, checks_table_total, out)
        print(f"[{rc_name}] wrote {rows} rows → {results_table}")

    print(f"TOTAL rows written: {grand_total}")


# ---- run it (only specify the YAML here) ----
run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # exclude_cols=["_created_date","_last_updated_date"]
)