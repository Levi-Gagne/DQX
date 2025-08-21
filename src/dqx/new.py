So i made the updates and this is what im running: 
# ======================================================================
# file: src/dqx/notebooks/02_run_dqx_checks.py
# ======================================================================

# Databricks notebook: 02_run_dqx_checks
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import json
from functools import reduce

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T

from resources.dqx_functions_0_8_0 import EXPECTED as EXPECTED_ARGS

from utils.color import Color
from utils.runtime import print_notebook_env
from utils.display import show_df, display_section

from utils.config import ProjectConfig, ConfigError, must, default_for_column, find_table
from utils.write import TableWriter, write_aligned
from utils.dqx import (
    empty_issues_array,
    normalize_issues_for_fp,
    apply_rules_isolating_failures,
    coerce_arguments,
)

# ---------------- Grouping & load rules from checks_config ----------------
def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for r in rules: out.setdefault(r["table_name"], []).append(r)
    return out

def _load_checks_from_table_as_dicts(
    spark: SparkSession,
    checks_table: str,
    run_config_name: str,
    coercion_mode: str = "permissive",
) -> Tuple[Dict[str, List[dict]], int, int]:
    df = (
        spark.table(checks_table)
        .where((F.col("run_config_name") == run_config_name) & (F.col("active") == True))
        .select("table_name", "name", "criticality", "filter", "run_config_name", "user_metadata", "check")
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]

    raw_rules: List[dict] = []; coerced: int = 0
    for r in rows:
        chk = r.get("check") or {}; fn = chk.get("function")
        args, _errs = coerce_arguments(EXPECTED_ARGS, chk.get("arguments"), fn, mode=coercion_mode)
        coerced += 1
        raw_rules.append({
            "table_name": r["table_name"],
            "name": r["name"],
            "criticality": r["criticality"],
            "run_config_name": r["run_config_name"],
            "filter": r.get("filter"),
            "user_metadata": r.get("user_metadata"),
            "check": {"function": fn, "for_each_column": chk.get("for_each_column") or None, "arguments": args},
        })

    status = DQEngine.validate_checks(raw_rules)
    if getattr(status, "has_errors", False):
        keep: List[dict] = []
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if not getattr(st, "has_errors", False): keep.append(r)
        skipped = len(raw_rules) - len(keep)
        return _group_by_table(keep), coerced, skipped
    return _group_by_table(raw_rules), coerced, 0

# ---------------- Row projection & embed check_id ----------------
def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
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
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

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

    issue_struct_type = T.StructType([
        T.StructField("name", T.StringType(), True),
        T.StructField("message", T.StringType(), True),
        T.StructField("columns", T.ArrayType(T.StringType()), True),
        T.StructField("filter", T.StringType(), True),
        T.StructField("function", T.StringType(), True),
        T.StructField("run_time", T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id", T.StringType(), True),
    ])

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

# ---------------- Summaries (one-pass cached) ----------------
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
            .collect()[0])
    rules = (annot
             .selectExpr(f"inline_outer(array_union(transform({err}, x -> x.name), transform({wrn}, x -> x.name))) as nm")
             .where(F.col("nm").isNotNull())
             .agg(F.countDistinct("nm").alias("rules"))
             .collect()[0]["rules"])
    annot.unpersist()

    return Row(table_name=table_name,
               table_total_rows=int(tot),
               table_total_error_rows=int(sums["e"]),
               table_total_warning_rows=int(sums["w"]),
               total_flagged_rows=int(sums["f"]),
               distinct_rules_fired=int(rules))

def _rules_hits_for_table(annot: DataFrame, table_name: str) -> DataFrame:
    err = "_errors" if "_errors" in annot.columns else "_error"
    wrn = "_warnings" if "_warnings" in annot.columns else "_warning"

    errs = (annot.select(F.explode_outer(F.expr(f"transform({err}, x -> x.name)")).alias("name"))
                  .where(F.col("name").isNotNull()).withColumn("severity", F.lit("error")))
    warns = (annot.select(F.explode_outer(F.expr(f"transform({wrn}, x -> x.name)")).alias("name"))
                   .where(F.col("name").isNotNull()).withColumn("severity", F.lit("warning")))
    both = errs.unionByName(warns, allowMissingColumns=True)
    return both.groupBy("name","severity").agg(F.count(F.lit(1)).alias("rows_flagged")).withColumn("table_name", F.lit(table_name))

# ---------------- Runner ----------------
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive",
) -> Dict[str, Any]:

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    local_tz = must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    print_notebook_env(spark, local_timezone=local_tz)

    nb = cfg.notebook(notebook_idx)

    # Resolve checks_config (explicit override respected)
    checks_table = find_table(cfg, "checks_config")

    # Targets
    targets = nb.targets()
    t1 = targets.target_table(1)
    results_fqn   = t1.full_table_name()
    results_write = must(t1.get("write"),   f"{results_fqn}.write")
    results_mode  = must(results_write.get("mode"),   f"{results_fqn}.write.mode")
    results_fmt   = must(results_write.get("format"), f"{results_fqn}.write.format")
    results_opts  = results_write.get("options") or {}
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_pk    = must(t1.get("primary_key"), f"{results_fqn}.primary_key")
    results_cols  = must(t1.get("columns"),     f"{results_fqn}.columns")
    created_by_default = default_for_column(results_cols, "created_by") or "AdminUser"

    # Optional summaries
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    summary_by_rule_fqn  = t2.full_table_name() if t2 else None
    summary_by_rule_mode = must(t2.get("write").get("mode"),   f"{summary_by_rule_fqn}.write.mode") if t2 else "overwrite"
    summary_by_rule_fmt  = must(t2.get("write").get("format"), f"{summary_by_rule_fqn}.write.format") if t2 else "delta"
    summary_by_rule_opts = (t2.get("write").get("options") or {}) if t2 else {}

    summary_by_table_fqn  = t3.full_table_name() if t3 else None
    summary_by_table_mode = must(t3.get("write").get("mode"),   f"{summary_by_table_fqn}.write.mode") if t3 else "overwrite"
    summary_by_table_fmt  = must(t3.get("write").get("format"), f"{summary_by_table_fqn}.write.format") if t3 else "delta"
    summary_by_table_opts = (t3.get("write").get("options") or {}) if t3 else {}

    # Ensure results table exists
    apply_meta = bool(must(cfg.get("project_config.apply_table_metadata"), "project_config.apply_table_metadata"))
    tw = TableWriter(spark)
    tw.create_table(
        fqn=results_fqn,
        columns=results_cols,
        format=results_fmt,
        options=results_opts,
        partition_by=results_part,
        primary_key=results_pk,
        apply_metadata=apply_meta,
        table_comment=results_desc,
        table_tags=t1.get("table_tags"),
    )

    dq = DQEngine(WorkspaceClient())

    try:
        checks_table_total = spark.table(checks_table).where(F.col("active") == True).count()
    except Exception:
        checks_table_total = -1

    grand_total = 0
    all_tbl_summaries: List[Row] = []
    printed_grand_once = False

    rc_map: Dict[str, Any] = cfg.get("run_config_name") or {}

    for rc_name, _rc_cfg in sorted(rc_map.items(), key=lambda kv: str(kv[0])):
        if rc_name is None or str(rc_name).lower() == "none": continue

        display_section(f"Run config: {rc_name}")

        by_tbl, coerced, skipped = _load_checks_from_table_as_dicts(spark, checks_table, rc_name, coercion_mode=coercion_mode)
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
                annot, bad = apply_rules_isolating_failures(dq, src, tbl_rules, tbl)
                if annot is None: continue
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

            out_batches.append(_project_row_hits(annot, tbl, rc_name, created_by_default, exclude_cols=exclude_cols))

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
            counts = (rules_all.groupBy("table_name","name","severity").agg(F.sum("rows_flagged").alias("rows_flagged")).withColumnRenamed("name","rule_name"))
            full_rules = (cfg_rules.join(counts, on=["table_name","rule_name","severity"], how="left").withColumn("rows_flagged", F.coalesce(F.col("rows_flagged"), F.lit(0))))
            totals_df = spark.createDataFrame([Row(table_name=k, table_total_rows=v) for k, v in table_row_counts.items()])
            full_rules = (
                full_rules.join(totals_df, "table_name", "left")
                .withColumn("pct_of_table_rows", F.when(F.col("table_total_rows") > 0, F.col("rows_flagged") / F.col("table_total_rows")).otherwise(F.lit(0.0)))
                .select("table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
                .orderBy(F.desc("rows_flagged"), F.asc("table_name"), F.asc("rule_name"))
            )
            display_section(f"Row-hit summary by rule (run_config={rc_name})")
            show_df(full_rules, n=2000, truncate=False)
            (full_rules
             .withColumn("run_config_name", F.lit(rc_name))
             .select("run_config_name","table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
             .write.format(summary_by_rule_fmt).mode(summary_by_rule_mode).options(**summary_by_rule_opts).saveAsTable(summary_by_rule_fqn))

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

        # Align + write (mergeSchema default for Delta)
        write_block = {
            "mode": results_mode,
            "format": results_fmt,
            "options": ({"mergeSchema": "true", **results_opts} if results_fmt.lower()=="delta" else results_opts),
        }
        write_aligned(spark, out, fqn=results_fqn, write_block=write_block)

        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # summary by table (ALL run_configs — write once)
        if summary_by_table_fqn and not printed_grand_once:
            grand_df = (
                spark.createDataFrame(all_tbl_summaries)
                .select("run_config_name","table_name","table_total_rows","table_total_error_rows","table_total_warning_rows","total_flagged_rows","distinct_rules_fired")
                .orderBy("run_config_name","table_name")
            )
            display_section("Row-hit summary by table (ALL run_configs)")
            show_df(grand_df, n=500, truncate=False)
            (grand_df.write.format(summary_by_table_fmt).mode(summary_by_table_mode).options(**summary_by_table_opts).saveAsTable(summary_by_table_fqn))
            printed_grand_once = True

    display_section("Grand total")
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

    return {"results_table": results_fqn, "grand_total_rows": grand_total, "checks_table": checks_table, "notebook_idx": notebook_idx}

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = ProjectConfig("resources/dqx_config.yaml", spark=spark)
    result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2, coercion_mode="strict")
    print(result)




HEre is the error:
[runtime] Non-fatal while printing runtime info: Invalid timezone 'America/Chicago'. Must be a valid IANA timezone string. Examples: 'UTC', 'America/Chicago', 'Europe/Berlin'.

════════════════════════════════════════════════════════════════════════════════
║ Run config: default
════════════════════════════════════════════════════════════════════════════════
[default] checks_in_table_total=79, loaded=36, coerced=36, skipped_invalid=0


[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "inline(array_union(transform(_errors, lambdafunction(namedlambdavariable().name, namedlambdavariable())), transform(_warnings, lambdafunction(namedlambdavariable().name, namedlambdavariable()))))" due to data type mismatch: The first parameter requires the "ARRAY<STRUCT>" type, however "array_union(transform(_errors, lambdafunction(namedlambdavariable().name, namedlambdavariable())), transform(_warnings, lambdafunction(namedlambdavariable().name, namedlambdavariable())))" has the type "ARRAY<STRING>". SQLSTATE: 42K09
File <command-4730582016133245>, line 463
    461 spark = SparkSession.builder.getOrCreate()
    462 cfg = ProjectConfig("resources/dqx_config.yaml", spark=spark)
--> 463 result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2, coercion_mode="strict")
    464 print(result)
File <command-4730582016133245>, line 374, in run_checks_loader(spark, cfg, notebook_idx, exclude_cols, coercion_mode)
    371 total_rows = annot.count()
    372 table_row_counts[tbl] = total_rows
--> 374 summary_row = _summarize_table(annot, tbl)
    375 rc_tbl_summaries.append(summary_row)
    376 all_tbl_summaries.append(Row(run_config_name=rc_name, **summary_row.asDict()))
File <command-4730582016133245>, line 244, in _summarize_table(annot, table_name)
    231 tot = annot.count()
    232 sums = (annot
    233         .select(
    234             (F.size(F.col(err)) > 0).cast("int").alias("e"),
   (...)
    238         .agg(F.sum("e").alias("e"), F.sum("w").alias("w"), F.sum("f").alias("f"))
    239         .collect()[0])
    240 rules = (annot
    241          .selectExpr(f"inline_outer(array_union(transform({err}, x -> x.name), transform({wrn}, x -> x.name))) as nm")
    242          .where(F.col("nm").isNotNull())
    243          .agg(F.countDistinct("nm").alias("rules"))
--> 244          .collect()[0]["rules"])
    245 annot.unpersist()
    247 return Row(table_name=table_name,
    248            table_total_rows=int(tot),
    249            table_total_error_rows=int(sums["e"]),
    250            table_total_warning_rows=int(sums["w"]),
    251            total_flagged_rows=int(sums["f"]),
    252            distinct_rules_fired=int(rules))





Can you resolve this? 
