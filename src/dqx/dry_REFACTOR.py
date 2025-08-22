# Databricks notebook: 01_load_dqx_checks
# Purpose: Load YAML rules into dq_{env}.dqx.checks_config
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations

import json, yaml, hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

from pyspark.sql import SparkSession, DataFrame, types as T, functions as F
from databricks.labs.dqx.engine import DQEngine

from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.color import Color
from utils.console import Console
from utils.config import ProjectConfig, ConfigError, must
from utils.write import TableWriter, write_aligned
from utils.path import dbfs_to_local, list_yaml_files
from utils.table import struct_to_columns_spec  # <-- reusable converter

# Force UTC for storage/compute
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================
# SPARK STRUCTURED SCHEMA (source of truth)
# =========================
CHECKS_CONFIG_STRUCT = T.StructType([
    T.StructField("check_id",         T.StringType(),   False, {"comment": "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}."}),
    T.StructField("check_id_payload", T.StringType(),   False, {"comment": "Canonical JSON used to derive `check_id` (sorted keys, normalized values)."}),
    T.StructField("table_name",       T.StringType(),   False, {"comment": "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability."}),
    T.StructField("name",             T.StringType(),   False, {"comment": "Human-readable rule name. Used in UI/diagnostics and joins."}),
    T.StructField("criticality",      T.StringType(),   False, {"comment": "Rule severity: `error|warn`."}),
    T.StructField("check", T.StructType([
        T.StructField("function",        T.StringType(), False, {"comment": "DQX function to run"}),
        T.StructField("for_each_column", T.ArrayType(T.StringType()), True,  {"comment": "Optional list of columns"}),
        T.StructField("arguments",       T.MapType(T.StringType(), T.StringType()), True, {"comment": "Key/value args"}),
    ]), False, {"comment": "Structured rule `{function, for_each_column?, arguments?}`; values stringified."}),
    T.StructField("filter",           T.StringType(),   True,  {"comment": "Optional SQL predicate applied before evaluation (row-level)."}),
    T.StructField("run_config_name",  T.StringType(),   False, {"comment": "Execution group/tag. Not part of identity."}),
    T.StructField("user_metadata",    T.MapType(T.StringType(), T.StringType()), True, {"comment": "Free-form map<string,string>."}),
    T.StructField("yaml_path",        T.StringType(),   False, {"comment": "Absolute/volume path to the defining YAML doc (lineage)."}),
    T.StructField("active",           T.BooleanType(),  False, {"comment": "If `false`, rule is ignored by runners."}),
    T.StructField("created_by",       T.StringType(),   False, {"comment": "Audit: creator/principal that materialized the row."}),
    T.StructField("created_at",       T.TimestampType(),False, {"comment": "Audit: creation timestamp (UTC)."}),
    T.StructField("updated_by",       T.StringType(),   True,  {"comment": "Audit: last updater (nullable)."}),
    T.StructField("updated_at",       T.TimestampType(),True,  {"comment": "Audit: last update timestamp (UTC, nullable)."}),
])

# Optional: comments map (add/override comments without touching the struct)
CHECKS_CONFIG_COMMENTS: Dict[str, str] = {
    # "check_id": "Your custom comment here (overrides struct metadata)",
}

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
    payload_obj = {"table_name": (table_name or "").lower(), "filter": _canon_filter(filter_str), "check": _canon_check(check_dict or {})}
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
        if not d:
            continue
        if isinstance(d, dict):
            out.append(d)
        elif isinstance(d, list):
            out.extend([x for x in d if isinstance(x, dict)])
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
# Build rows from YAML docs (UTC timestamps)
# =========================
def process_yaml_file(
    path: str,
    required_fields: List[str],
    created_by_value: str,
    allowed_criticality: List[str],
) -> List[dict]:
    docs = load_yaml_rules(path)
    if not docs:
        print(f"{Console.SKIP} {path} has no rules.")
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

        # UTC TimestampType (session is UTC)
        created_at_ts = datetime.utcnow()

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
            "active": bool(rule.get("active", True)),
            "created_by": created_by_value,
            "created_at": created_at_ts,
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
        if len(lst) == 1:
            out.append(lst[0]); continue
        lst = sorted(lst, key=lambda x: (x.get("yaml_path",""), x.get("name","")))
        keep, dups = lst[0], lst[1:]; dropped += len(dups)
        head = f"{Console.DEDUPE} {len(dups)} duplicate(s) for check_id={cid[:12]}…"
        lines = ["    " + _fmt_rule_for_dup(x) for x in lst]
        tail = f"    -> keeping: name={keep.get('name')} | file={keep.get('yaml_path')}"
        blocks.append("\n".join([head, *lines, tail])); out.append(keep)
    if dropped:
        msg = "\n\n".join(blocks) + f"\n{Console.DEDUPE} total dropped={dropped}"
        if mode == "error": raise ValueError(msg)
        if mode == "warn": print(msg)
    return out

# =========================
# Discover rule YAMLs
# =========================
def discover_yaml(cfg: ProjectConfig, rules_dir: str) -> List[str]:
    print(f"{Console.DEBUG} rules_dir (raw from YAML): {rules_dir}")
    files = list_yaml_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    df = SparkSession.builder.getOrCreate().createDataFrame([(p,) for p in files], "yaml_path string")
    show_df(df, n=500, truncate=False)
    return files

# =========================
# Build DF & alignment
# =========================
def build_df_from_rules(spark: SparkSession, rules: List[dict]) -> DataFrame:
    return spark.createDataFrame(rules, schema=CHECKS_CONFIG_STRUCT)

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

    # Only print for human eyes (local time)
    local_tz    = must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    print_notebook_env(spark, local_timezone=local_tz)

    proc_tz     = must(cfg.get("project_config.processing_timezone"), "project_config.processing_timezone")
    apply_meta  = bool(must(cfg.get("project_config.apply_table_metadata"), "project_config.apply_table_metadata"))
    dedupe_mode = must(cfg.get("project_config.batch_dedupe_mode"), "project_config.batch_dedupe_mode")

    nb = cfg.notebook(notebook_idx)
    ds = must(nb.get("data_source"), f"notebooks.notebook_{notebook_idx}.data_source")
    rules_dir       = must(ds.get("source_path"),         f"notebooks.notebook_{notebook_idx}.data_source.source_path")
    allowed_crit    = must(ds.get("allowed_criticality"), f"notebooks.notebook_{notebook_idx}.data_source.allowed_criticality")
    required_fields = must(ds.get("required_fields"),     f"notebooks.notebook_{notebook_idx}.data_source.required_fields")

    t = nb.targets().target_table(1)
    fqn           = t.full_table_name()
    partition_by  = t.get("partition_by") or []
    write_block   = must(t.get("write"), f"{fqn}.write")
    table_comment = t.get("table_description")
    table_tags    = t.get("table_tags")
    primary_key   = must(t.get("primary_key"), f"{fqn}.primary_key")

    # Build table metadata from struct (+ optional comments map)
    columns_spec = struct_to_columns_spec(CHECKS_CONFIG_STRUCT, comments=CHECKS_CONFIG_COMMENTS)

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
        print(f"{Console.VALIDATION} Validation only: not writing any rules.")
        errs: List[str] = []
        for p in yaml_files:
            try:
                validate_rules_file(load_yaml_rules(p), p)
            except Exception as e:
                errs.append(f"{p}: {e}")
        return {"config_path": cfg.path, "rules_files": len(yaml_files), "errors": errs}

    # Build rows (UTC created_at), dedupe by check_id
    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(
            full_path,
            required_fields=required_fields,
            created_by_value="AdminUser",  # settable later if you want
            allowed_criticality=allowed_crit,
        )
        if file_rules:
            all_rules.extend(file_rules)
            print(f"{Console.LOADER} {full_path}: rules={len(file_rules)}")

    pre_dedupe = len(all_rules)
    rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=dedupe_mode)
    post_dedupe = len(rules)

    if not rules:
        print(f"{Console.SKIP} No rules discovered; nothing to do.")
        return {"config_path": cfg.path, "rules_files": len(yaml_files), "wrote_rows": 0, "target_table": fqn}

    print(f"{Console.DEDUPE} total parsed rules (pre-dedupe): {pre_dedupe}")
    df = build_df_from_rules(spark, rules)

    display_section("SUMMARY OF RULES LOADED FROM YAML")
    totals = [(df.count(), df.select("check_id").distinct().count(), df.select("check_id", "run_config_name").distinct().count())]
    tdf = df.sparkSession.createDataFrame(totals, schema="`total number of rules found` long, `unique rules found` long, `distinct pair of rules` long")
    show_df(tdf, n=1)

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

    table_fields = [f.name for f in spark.table(fqn).schema.fields]
    write_rules(spark, df.select(*table_fields), fqn=fqn, write_block=write_block)

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