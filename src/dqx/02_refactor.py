# Databricks notebook: 02_run_dqx_checks
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

import json
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession, DataFrame, Row, functions as F, types as T

from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED

from utils.color import Color
from utils.runtime import print_notebook_env
from utils.display import show_df, display_section
from utils.config import ProjectConfig, ConfigError
from utils.write import TableWriter

# =========================
# Small helpers
# =========================
def _must(val: Any, name: str) -> Any:
    if val is None:
        raise ConfigError(f"Missing required config: {name}")
    if isinstance(val, str) and not val.strip():
        raise ConfigError(f"Missing required config (empty string): {name}")
    return val

def _default_for_column(columns: Dict[str, Any], target_name: str) -> Optional[str]:
    for spec in (columns or {}).values():
        if isinstance(spec, dict) and (spec.get("name") or "").strip() == target_name:
            return spec.get("default_value")
    return None

def _as_target_index(k: Any) -> Optional[int]:
    # Accept 1, "1", "target_table_1"
    try:
        return int(k)
    except Exception:
        try:
            s = str(k)
            if "_" in s:
                return int(s.rsplit("_", 1)[-1])
        except Exception:
            pass
    return None

def _find_checks_config_table(cfg: ProjectConfig) -> str:
    # Iterate notebooks and their targets; return FQN whose table name is 'checks_config'
    for nb_key in cfg.list_notebooks():    # e.g., "notebook_1", "notebook_2"
        nb = cfg.notebook(nb_key)
        tcfg = nb.targets()
        for k in tcfg.keys():
            idx = _as_target_index(k)
            if idx is None:
                continue
            try:
                fqn = tcfg.target_table(idx).full_table_name()
            except Exception:
                continue
            if not fqn:
                continue
            last = fqn.split(".")[-1].strip().lower()
            if last == "checks_config":
                return fqn
    raise ValueError("Could not locate a target table named '*.*.checks_config' in the YAML config.")

# ============================================================================
# Argument parsing & coercion for DQX
# ============================================================================
def _parse_scalar(s: Optional[str]):
    if s is None: return None
    s = s.strip()
    sl = s.lower()
    if sl in ("null", "none", ""): return None
    if sl in ("true", "false"):
        return sl == "true"
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

# ============================================================================
# Load checks from config table
# ============================================================================
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
        .select("table_name", "name", "criticality", "filter", "run_config_name", "user_metadata", "check")
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
        for r in raw_rules:
            st = DQEngine.validate_checks([r])
            if not getattr(st, "has_errors", False):
                keep.append(r)
        skipped = len(raw_rules) - len(keep)
        return _group_by_table(keep), coerced, skipped
    else:
        return _group_by_table(raw_rules), coerced, 0

# ============================================================================
# Apply with isolation (diagnostics)
# ============================================================================
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

# ============================================================================
# Projection (row_log)
# ============================================================================
def _pick_col(df: DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _empty_issues_array() -> F.Column:
    issue_struct_type = T.StructType([
        T.StructField("name",          T.StringType(),  True),
        T.StructField("message",       T.StringType(),  True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(),  True),
        T.StructField("function",      T.StringType(),  True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(),  True),
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

# ============================================================================
# Embed check_id inside each issue element
# ============================================================================
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
            F.col("check_id").alias("cfg_check_id")
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
        T.StructField("name",          T.StringType(),  True),
        T.StructField("message",       T.StringType(),  True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(),  True),
        T.StructField("function",      T.StringType(),  True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(),  True),
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
                    F.col("issue_check_id").cast(T.StringType()).alias("check_id")
                )).alias("iss_json")
            )
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
        .withColumn("_errors", F.coalesce(F.col("_errors"), _empty_issues_array()))
        .withColumn("_warnings", F.coalesce(F.col("_warnings"), _empty_issues_array()))
    )

# ============================================================================
# Summaries
# ============================================================================
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

# ============================================================================
# Runner (index-based, config-driven)
# ============================================================================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,                 # ← use index to mirror the loader
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive",
) -> Dict[str, Any]:

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    local_tz = _must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    print_notebook_env(spark, local_timezone=local_tz)

    # Grab notebook by index
    nb = cfg.notebook(notebook_idx)

    # Resolve checks_config table across config
    checks_table = _find_checks_config_table(cfg)

    # Targets for this notebook
    targets = nb.targets()
    t1 = targets.target_table(1)  # row-level results (checks_log)
    results_fqn = t1.full_table_name()
    results_write = _must(t1.get("write"), f"{results_fqn}.write")
    results_mode  = _must(results_write.get("mode"),   f"{results_fqn}.write.mode")
    results_fmt   = _must(results_write.get("format"), f"{results_fqn}.write.format")
    results_opts  = results_write.get("options") or {}
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_pk    = _must(t1.get("primary_key"), f"{results_fqn}.primary_key")
    results_cols  = _must(t1.get("columns"), f"{results_fqn}.columns")
    created_by_default = _default_for_column(results_cols, "created_by") or "AdminUser"

    # Optional summaries (targets 2 and 3)
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    summary_by_rule_fqn  = t2.full_table_name() if t2 else None
    summary_by_rule_mode = _must(t2.get("write").get("mode"),   f"{summary_by_rule_fqn}.write.mode") if t2 else "overwrite"
    summary_by_rule_fmt  = _must(t2.get("write").get("format"), f"{summary_by_rule_fqn}.write.format") if t2 else "delta"
    summary_by_rule_opts = (t2.get("write").get("options") or {}) if t2 else {}

    summary_by_table_fqn  = t3.full_table_name() if t3 else None
    summary_by_table_mode = _must(t3.get("write").get("mode"),   f"{summary_by_table_fqn}.write.mode") if t3 else "overwrite"
    summary_by_table_fmt  = _must(t3.get("write").get("format"), f"{summary_by_table_fqn}.write.format") if t3 else "delta"
    summary_by_table_opts = (t3.get("write").get("options") or {}) if t3 else {}

    # Create results table from YAML spec (idempotent)
    apply_meta = bool(_must(cfg.get("project_config.apply_table_metadata"), "project_config.apply_table_metadata"))
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

    for rc_name, _rc_cfg in rc_map.items():
        if rc_name is None or str(rc_name).lower() == "none":
            continue

        display_section(f"Run config: {rc_name}")

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

            row_hits = _project_row_hits(annot, tbl, rc_name, created_by_default, exclude_cols=exclude_cols)
            if row_hits.limit(1).count() > 0:
                out_batches.append(row_hits)

        if rc_tbl_summaries:
            summary_df = spark.createDataFrame(rc_tbl_summaries).orderBy("table_name")
            display_section(f"Row-hit summary by table (run_config={rc_name})")
            show_df(summary_df, n=200, truncate=False)

        # summary by rule
        if rc_rule_hit_parts and summary_by_rule_fqn:
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
                    F.when(F.lower("criticality").isin("warn", "warning"), F.lit("warning")).otherwise(F.lit("error")).alias("severity")
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
                cfg_rules.join(counts, on=["table_name","rule_name","severity"], how="left")
                .withColumn("rows_flagged", F.coalesce(F.col("rows_flagged"), F.lit(0)))
            )

            totals_df = spark.createDataFrame([Row(table_name=k, table_total_rows=v) for k, v in table_row_counts.items()])
            full_rules = (
                full_rules.join(totals_df, "table_name", "left")
                .withColumn("pct_of_table_rows", F.when(F.col("table_total_rows") > 0, F.col("rows_flagged") / F.col("table_total_rows")).otherwise(F.lit(0.0)))
                .select("table_name", "rule_name", "severity", "rows_flagged", "table_total_rows", "pct_of_table_rows")
                .orderBy(F.desc("rows_flagged"), F.asc("table_name"), F.asc("rule_name"))
            )

            display_section(f"Row-hit summary by rule (run_config={rc_name})")
            show_df(full_rules, n=2000, truncate=False)

            (full_rules
             .withColumn("run_config_name", F.lit(rc_name))
             .select("run_config_name","table_name","rule_name","severity","rows_flagged","table_total_rows","pct_of_table_rows")
             .write
             .format(summary_by_rule_fmt)
             .mode(summary_by_rule_mode)
             .options(**summary_by_rule_opts)
             .saveAsTable(summary_by_rule_fqn))

        if not out_batches:
            print(f"[{rc_name}] no row-level hits.")
            continue

        out = out_batches[0]
        for b in out_batches[1:]:
            out = out.unionByName(b, allowMissingColumns=True)

        out = _embed_issue_check_ids(out, checks_table)

        # Align to declared schema (results table)
        target_cols = [f.name for f in spark.table(results_fqn).schema.fields]
        out = out.select(*target_cols)

        # Write via TableWriter
        tw.write_only(
            out,
            fqn=results_fqn,
            mode=results_mode,
            format=results_fmt,
            options=results_opts,
        )
        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # summary by table (all run_configs — write once)
        if summary_by_table_fqn and not printed_grand_once:
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

            (grand_df
             .write
             .format(summary_by_table_fmt)
             .mode(summary_by_table_mode)
             .options(**summary_by_table_opts)
             .saveAsTable(summary_by_table_fqn))
            printed_grand_once = True

    display_section("Grand total")
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

    return {
        "results_table": results_fqn,
        "grand_total_rows": grand_total,
        "checks_table": checks_table,
        "notebook_idx": notebook_idx,
    }

# ============================================================================
# Minimal entrypoint
# ============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = ProjectConfig("resources/dqx_config.yaml", spark=spark)
    result = run_checks_loader(
        spark=spark,
        cfg=cfg,
        notebook_idx=2,                # ← notebook_2: 02_run_dqx_checks
        # exclude_cols=["_created_date","_last_updated_date"],
        coercion_mode="strict",
    )
    print(result)