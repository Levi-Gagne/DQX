# ======================================================================
# file: src/dqx/notebooks/01_load_dqx_checks.py
# ======================================================================

# Databricks notebook: 01_load_dqx_checks
# Purpose: Load YAML rules into dq_{env}.dqx.checks_config
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations

import json, hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

import yaml
from pyspark.sql import SparkSession, DataFrame
from databricks.labs.dqx.engine import DQEngine

from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.color import Color

from dqx.utils.config import ProjectConfig, ConfigError, must, default_for_column
from dqx.utils.write import TableWriter, write_aligned
from utils.path import dbfs_to_local, list_yaml_files

# =========================
# Column specs helpers
# =========================
def _timestamp_columns_from_spec(columns: Dict[str, Any]) -> List[str]:
    return [
        (spec.get("name") or "").strip()
        for spec in columns.values()
        if isinstance(spec, dict) and (spec.get("data_type") or "").lower() == "timestamp" and (spec.get("name") or "").strip()
    ]

def _coerce_rows_for_timestamp_fields(rows: List[dict], ts_fields: List[str]) -> None:
    for r in rows:
        for c in ts_fields:
            v = r.get(c, None)
            if isinstance(v, str) and v:
                try:
                    dt = datetime.fromisoformat(v)
                except Exception:
                    dt = datetime.fromisoformat(v[:-1] + "+00:00") if v.endswith("Z") else None
                if dt is None:
                    raise
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                r[c] = dt

# =========================
# Canonicalization & IDs
# =========================
def _canon_filter(s: Optional[str]) -> str:
    return "" if not s else " ".join(str(s).split())

def _stringify_map_values(d: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (d or {}).items():
        if isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif v is None:
            out[k] = "null"
        else:
            out[k] = str(v)
    return out

def _canon_check(chk: Dict[str, Any]) -> Dict[str, Any]:
    out = {"function": chk.get("function"), "for_each_column": None, "arguments": {}}
    fec = chk.get("for_each_column")
    if isinstance(fec, list):
        out["for_each_column"] = sorted([str(x) for x in fec]) or None
    args = chk.get("arguments") or {}
    canon_args: Dict[str, str] = {}
    for k, v in args.items():
        sv = "" if v is None else str(v).strip()
        if (sv.startswith("{") and sv.endswith("}")) or (sv.startswith("[") and sv.endswith("]")):
            try:
                sv = json.dumps(json.loads(sv), sort_keys=True, separators=(",", ":"))
            except Exception:
                pass
        canon_args[str(k)] = sv
    out["arguments"] = {k: canon_args[k] for k in sorted(canon_args)}
    return out

def compute_check_id_payload(table_name: str, check_dict: Dict[str, Any], filter_str: Optional[str]) -> str:
    payload_obj = {
        "table_name": (table_name or "").lower(),
        "filter": _canon_filter(filter_str),
        "check": _canon_check(check_dict or {}),
    }
    return json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))

def compute_check_id_from_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()

# =========================
# Rule YAML load/validate
# =========================
def load_yaml_rules(path: str) -> List[dict]:
    p = Path(dbfs_to_local(path))
    if not p.exists():
        raise FileNotFoundError(f"Rules YAML not found: {p}")
    with open(p, "r") as fh:
        docs = list(yaml.safe_load_all(fh)) or []
    out: List[dict] = []
    for d in docs:
        if not d: continue
        out.extend(d if isinstance(d, list) else [d] if isinstance(d, dict) else [])
    return out

def validate_rules_file(rules: List[dict], file_path: str):
    if not rules:
        raise ValueError(f"No rules found in {file_path} (empty or invalid YAML).")
    probs, seen = [], set()
    for r in rules:
        nm = r.get("name")
        if not nm: probs.append(f"Missing rule name in {file_path}")
        if nm in seen: probs.append(f"Duplicate rule name '{nm}' in {file_path}")
        seen.add(nm)
    if probs: raise ValueError(f"File-level validation failed in {file_path}: {probs}")

def validate_rule_fields(rule: dict, file_path: str, required_fields: List[str], allowed_criticality: List[str]):
    probs = []
    for f in required_fields:
        if not rule.get(f): probs.append(f"Missing required field '{f}' in rule '{rule.get('name')}' ({file_path})")
    if rule.get("table_name", "").count(".") != 2:
        probs.append(f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})")
    if rule.get("criticality") not in set(allowed_criticality):
        probs.append(f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})")
    if not rule.get("check", {}).get("function"):
        probs.append(f"Missing check.function in rule '{rule.get('name')}' ({file_path})")
    if probs: raise ValueError("Rule-level validation failed: " + "; ".join(probs))

def validate_with_dqx(rules: List[dict], file_path: str):
    status = DQEngine.validate_checks(rules)
    if getattr(status, "has_errors", False):
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")

# =========================
# Build rows from YAML docs
# =========================
def process_yaml_file(
    path: str,
    required_fields: List[str],
    time_zone: str,
    created_by_value: str,
    allowed_criticality: List[str],
) -> List[dict]:
    docs = load_yaml_rules(path)
    if not docs:
        print(f"[skip] {path} has no rules.")
        return []

    validate_rules_file(docs, path)
    flat: List[dict] = []

    for rule in docs:
        validate_rule_fields(rule, path, required_fields, allowed_criticality)
        raw_check = rule["check"] or {}
        payload   = compute_check_id_payload(rule["table_name"], raw_check, rule.get("filter"))
        check_id  = compute_check_id_from_payload(payload)

        function = raw_check.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}').")

        for_each = raw_check.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').")
        for_each = [str(x) for x in (for_each or [])] or None

        arguments = raw_check.get("arguments", {}) or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{path}: check.arguments must be a map (rule '{rule.get('name')}').")
        arguments = _stringify_map_values(arguments)

        user_metadata = rule.get("user_metadata")
        if user_metadata is not None:
            if not isinstance(user_metadata, dict):
                raise ValueError(f"{path}: user_metadata must be a map (rule '{rule.get('name')}').")
            user_metadata = _stringify_map_values(user_metadata)

        created_at_iso = (
            datetime.now(ZoneInfo(time_zone)).isoformat()
            if ZoneInfo is not None else datetime.now(timezone.utc).isoformat()
        )

        flat.append({
            "check_id": check_id,
            "check_id_payload": payload,
            "table_name": rule["table_name"],
            "name": rule["name"],
            "criticality": rule["criticality"],
            "check": {"function": function, "for_each_column": for_each, "arguments": arguments or None},
            "filter": rule.get("filter"),
            "run_config_name": rule["run_config_name"],
            "user_metadata": user_metadata or None,
            "yaml_path": path,
            "active": rule.get("active", True),
            "created_by": created_by_value,
            "created_at": created_at_iso,
            "updated_by": None,
            "updated_at": None,
        })

    validate_with_dqx(docs, path)
    return flat

# =========================
# Dedupe (on check_id)
# =========================
def _fmt_rule_for_dup(r: dict) -> str:
    return f"name={r.get('name')} | file={r.get('yaml_path')} | criticality={r.get('criticality')} | run_config={r.get('run_config_name')} | filter={r.get('filter')}"

def dedupe_rules_in_batch_by_check_id(rules: List[dict], mode: str) -> List[dict]:
    groups: Dict[str, List[dict]] = {}
    for r in rules: groups.setdefault(r["check_id"], []).append(r)
    out: List[dict] = []; dropped = 0; blocks: List[str] = []
    for cid, lst in groups.items():
        if len(lst) == 1: out.append(lst[0]); continue
        lst = sorted(lst, key=lambda x: (x.get("yaml_path",""), x.get("name","")))
        keep, dups = lst[0], lst[1:]; dropped += len(dups)
        head = f"[dup/batch/check_id] {len(dups)} duplicate(s) for check_id={cid[:12]}…"
        lines = ["    " + _fmt_rule_for_dup(x) for x in lst]
        tail = f"    -> keeping: name={keep.get('name')} | file={keep.get('yaml_path')}"
        blocks.append("\n".join([head, *lines, tail])); out.append(keep)
    if dropped:
        msg = "\n\n".join(blocks) + f"\n[dedupe/batch] total dropped={dropped}"
        if mode == "error": raise ValueError(msg)
        if mode == "warn": print(msg)
    return out

# =========================
# Discover rule YAMLs
# =========================
def discover_yaml(cfg: ProjectConfig, rules_dir: str) -> List[str]:
    print("[debug] rules_dir (raw from YAML):", rules_dir)
    files = list_yaml_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    df = SparkSession.builder.getOrCreate().createDataFrame([(p,) for p in files], "yaml_path string")
    show_df(df, n=500, truncate=False)
    return files

# =========================
# Build DF & alignment
# =========================
def build_df_from_rules(spark: SparkSession, rules: List[dict], columns_spec: Dict[str, Any]) -> DataFrame:
    ts_fields = _timestamp_columns_from_spec(columns_spec)
    if ts_fields: _coerce_rows_for_timestamp_fields(rules, ts_fields)
    tw = TableWriter(spark); schema = tw.schema_from_columns(columns_spec)
    return spark.createDataFrame(rules, schema=schema)

def preview_summary(df: DataFrame) -> None:
    display_section("SUMMARY OF RULES LOADED FROM YAML")
    totals = [(df.count(), df.select("check_id").distinct().count(), df.select("check_id", "run_config_name").distinct().count())]
    tdf = df.sparkSession.createDataFrame(totals, schema="`total number of rules found` long, `unique rules found` long, `distinct pair of rules` long")
    show_df(tdf, n=1)

# =========================
# Write
# =========================
def write_rules(spark: SparkSession, df: DataFrame, *, fqn: str, write_block: Dict[str, Any]) -> None:
    write_aligned(spark, df, fqn=fqn, write_block=write_block)

# =========================
# Runner
# =========================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    dry_run: bool = False,
    validate_only: bool = False,
) -> Dict[str, Any]:

    local_tz    = must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    proc_tz     = must(cfg.get("project_config.processing_timezone"), "project_config.processing_timezone")
    apply_meta  = bool(must(cfg.get("project_config.apply_table_metadata"), "project_config.apply_table_metadata"))
    dedupe_mode = must(cfg.get("project_config.batch_dedupe_mode"), "project_config.batch_dedupe_mode")

    print_notebook_env(spark, local_timezone=local_tz)

    nb = cfg.notebook(notebook_idx)
    ds = must(nb.get("data_source"), f"notebooks.notebook_{notebook_idx}.data_source")
    rules_dir       = must(ds.get("source_path"),         f"notebooks.notebook_{notebook_idx}.data_source.source_path")
    allowed_crit    = must(ds.get("allowed_criticality"), f"notebooks.notebook_{notebook_idx}.data_source.allowed_criticality")
    required_fields = must(ds.get("required_fields"),     f"notebooks.notebook_{notebook_idx}.data_source.required_fields")

    t = nb.targets().target_table(1)
    fqn           = t.full_table_name()
    columns_spec  = must(t.get("columns"), f"{fqn}.columns")
    partition_by  = t.get("partition_by") or []
    write_block   = must(t.get("write"), f"{fqn}.write")
    table_comment = t.get("table_description")
    table_tags    = t.get("table_tags")
    primary_key   = must(t.get("primary_key"), f"{fqn}.primary_key")

    created_by_value = default_for_column(columns_spec, "created_by"); must(created_by_value, f"{fqn}.columns.created_by.default_value")

    tw = TableWriter(spark)
    tw.create_table(
        fqn=fqn,
        columns=columns_spec,
        format=must(write_block.get("format"), f"{fqn}.write.format"),
        options=write_block.get("options") or {},
        partition_by=partition_by,
        primary_key=primary_key,
        apply_metadata=apply_meta,
        table_comment=table_comment,
        table_tags=table_tags,
    )

    yaml_files = discover_yaml(cfg, rules_dir)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        errs: List[str] = []
        for p in yaml_files:
            try:
                validate_rules_file(load_yaml_rules(p), p)
            except Exception as e:
                errs.append(f"{p}: {e}")
        return {"config_path": cfg.path, "rules_files": len(yaml_files), "errors": errs}

    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(
            full_path,
            required_fields=required_fields,
            time_zone=proc_tz,
            created_by_value=created_by_value,
            allowed_criticality=allowed_crit,
        )
        if file_rules:
            all_rules.extend(file_rules)
            print(f"[loader] {full_path}: rules={len(file_rules)}")

    pre_dedupe = len(all_rules)
    rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=dedupe_mode)
    post_dedupe = len(rules)

    if not rules:
        print("No rules discovered; nothing to do.")
        return {"config_path": cfg.path, "rules_files": len(yaml_files), "wrote_rows": 0, "target_table": fqn}

    print(f"[loader] total parsed rules (pre-dedupe): {pre_dedupe}")
    df = build_df_from_rules(spark, rules, columns_spec)
    preview_summary(df)

    if dry_run:
        display_section("DRY-RUN: FULL RULES PREVIEW")
        show_df(df.orderBy("table_name", "name"), n=1000, truncate=False)
        return {
            "config_path": cfg.path,
            "rules_files": len(yaml_files),
            "rules_pre_dedupe": pre_dedupe,
            "rules_post_dedupe": post_dedupe,
            "unique_check_ids": df.select("check_id").distinct().count(),
            "distinct_rule_run_pairs": df.select("check_id","run_config_name").distinct().count(),
            "target_table": fqn,
            "wrote_rows": 0,
            "primary_key": primary_key,
            "write_mode": must(write_block.get("mode"), f"{fqn}.write.mode"),
        }

    write_rules(spark, df.select(*[f.name for f in spark.table(fqn).schema.fields]), fqn=fqn, write_block=write_block)

    wrote_rows = df.count()
    display_section("WRITE RESULT")
    summary = spark.createDataFrame([(wrote_rows, fqn, must(write_block.get("mode"), f"{fqn}.write.mode"), f"pk_{primary_key}")],
                                    schema="`rules written` long, `target table` string, `mode` string, `constraint` string")
    show_df(summary, n=1)
    print(f"{Color.b}{Color.ivory}Finished writing rules to '{Color.r}{Color.b}{Color.i}{Color.sea_green}{fqn}{Color.r}{Color.b}{Color.ivory}'{Color.r}.")

    return {
        "config_path": cfg.path,
        "rules_files": len(yaml_files),
        "rules_pre_dedupe": pre_dedupe,
        "rules_post_dedupe": post_dedupe,
        "unique_check_ids": df.select("check_id").distinct().count(),
        "distinct_rule_run_pairs": df.select("check_id","run_config_name").distinct().count(),
        "target_table": fqn,
        "wrote_rows": wrote_rows,
        "primary_key": primary_key,
        "write_mode": must(write_block.get("mode"), f"{fqn}.write.mode"),
    }

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = ProjectConfig("resources/dqx_config.yaml", variables={})
    result = run_checks_loader(spark, cfg, notebook_idx=1, dry_run=False, validate_only=False)
    print(result)


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

from dqx.utils.config import ProjectConfig, ConfigError, must, default_for_column, find_table
from dqx.utils.write import TableWriter, write_aligned
from dqx.utils.dqx import (
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

    reserved = {e_name, w_name, "_errs", "_warns"] - {None} | exclude_cols
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


# ======================================================================
# file: src/dqx/utils/dqx.py
# ======================================================================

from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional

from pyspark.sql import DataFrame, functions as F, types as T
from databricks.labs.dqx.engine import DQEngine

# --------- Issue array schema helpers ----------
_ISSUE_STRUCT = T.StructType([
    T.StructField("name",          T.StringType(),  True),
    T.StructField("message",       T.StringType(),  True),
    T.StructField("columns",       T.ArrayType(T.StringType()), True),
    T.StructField("filter",        T.StringType(),  True),
    T.StructField("function",      T.StringType(),  True),
    T.StructField("run_time",      T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("check_id",      T.StringType(),  True),
])

def empty_issues_array() -> F.Column:
    return F.from_json(F.lit("[]"), T.ArrayType(_ISSUE_STRUCT))

def normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
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

# --------- Rule application with isolation ----------
def apply_rules_isolating_failures(
    dq: DQEngine, src: DataFrame, rules: List[dict], table_name: str
) -> Tuple[Optional[DataFrame], List[Tuple[str, str]]]:
    try:
        return dq.apply_checks_by_metadata(src, rules), []
    except Exception:
        bad: List[Tuple[str, str]] = []
        good: List[dict] = []
        for r in rules:
            try:
                dq.apply_checks_by_metadata(src, [r])
                good.append(r)
            except Exception as ex:
                bad.append((r.get("name") or "<unnamed>", str(ex)))
        if not good:
            return None, bad
        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception:
            return None, bad

# --------- Argument coercion (uses EXPECTED spec supplied by caller) ----------
def _parse_scalar(s: Optional[str]):
    if s is None: return None
    s = s.strip(); sl = s.lower()
    if sl in ("null","none",""): return None
    if sl == "true": return True
    if sl == "false": return False
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try:
            import json; return json.loads(s)
        except Exception:
            return s
    try:
        return int(s) if s.lstrip("+-").isdigit() else float(s)
    except Exception:
        return s

def _to_list(v):
    if v is None: return []
    if isinstance(v, list): return v
    if isinstance(v, str) and v.strip().startswith("["):
        try:
            import json; return json.loads(v)
        except Exception:
            return [v]
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
        if vl in ("true","t","1"): return True
        if vl in ("false","f","0"): return False
    return v

def coerce_arguments(
    expected_spec: Dict[tuple, Dict[str, str]] | Dict[str, Dict[str, str]],
    args_map: Optional[Dict[str, str]],
    function_name: Optional[str],
    *,
    mode: str = "permissive",
) -> Tuple[Dict[str, Any], List[str]]:
    if not args_map: return {}, []
    raw = {k:_parse_scalar(v) for k, v in args_map.items()}
    spec = expected_spec.get((function_name or "").strip(), {}) if isinstance(expected_spec, dict) else {}

    out: Dict[str, Any] = {}; errs: List[str] = []
    for k, v in raw.items():
        want = spec.get(k)
        if want == "list":
            out[k] = _to_list(v)
            if not isinstance(out[k], list): errs.append(f"key '{k}' expected list, got {type(out[k]).__name__}")
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


# ======================================================================
# (APPEND-ONLY) src/dqx/utils/write.py  -> add this helper at file bottom
# ======================================================================

from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame
# NOTE: assumes TableWriter + WriteConfigError are already defined in this module.
# If you house this helper elsewhere, update the import path in your notebooks.

def write_aligned(spark: SparkSession, df: DataFrame, *, fqn: str, write_block: Dict[str, Any]) -> None:
    """
    Align columns to target table schema then write via TableWriter using the provided write block.
    Adds mergeSchema=true automatically for Delta unless explicitly overridden.
    """
    target_cols = [f.name for f in spark.table(fqn).schema.fields]
    missing = [c for c in target_cols if c not in df.columns]
    extra   = [c for c in df.columns if c not in target_cols]
    if missing or extra:
        raise WriteConfigError(f"Schema mismatch.\nMissing in df: {missing}\nExtra in df: {extra}")

    mode = (write_block.get("mode") or "append")
    fmt  = (write_block.get("format") or "delta")
    opts = (write_block.get("options") or {})
    if fmt.lower() == "delta" and "mergeschema" not in {k.lower(): v for k, v in opts.items()}:
        opts = {"mergeSchema": "true", **opts}

    tw = TableWriter(spark)
    tw.write_only(df.select(*target_cols), fqn=fqn, mode=mode, format=fmt, options=opts)


# ======================================================================
# (APPEND-ONLY) tail of src/dqx/utils/config.py  -> add these helpers
# ======================================================================

# NOTE: ConfigError class already exists earlier in this module; don't re-declare.

from typing import Any, Dict, Optional

def must(val: Any, name: str) -> Any:
    if val is None or (isinstance(val, str) and not val.strip()):
        raise ConfigError(f"Missing required config: {name}")
    return val

def default_for_column(columns: Dict[str, Any], target_name: str) -> Optional[str]:
    for spec in (columns or {}).values():
        if isinstance(spec, dict) and (spec.get("name") or "").strip() == target_name:
            return spec.get("default_value")
    return None

def _as_target_index(k: Any) -> Optional[int]:
    try:
        return int(k)
    except Exception:
        try:
            s = str(k)
            if "_" in s and s.rsplit("_", 1)[-1].isdigit():
                return int(s.rsplit("_", 1)[-1])
        except Exception:
            pass
    return None

def find_table(cfg: "ProjectConfig", table_name: str) -> str:
    want = (table_name or "").strip().lower()

    # explicit override only for checks_config, if set:
    if want == "checks_config":
        explicit = (cfg.get("project_config") or {}).get("checks_config_table")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()

    for nb_key in cfg.list_notebooks():
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
            if fqn and fqn.split(".")[-1].strip().lower() == want:
                return fqn
    raise ValueError(f"Could not locate a target table named '*.*.{want}' in the YAML config.")