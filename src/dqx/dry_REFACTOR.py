# Databricks notebook: 01_load_dqx_checks
# Purpose: Load YAML rules into dq_{env}.dqx.checks_config
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations

import json, yaml, hashlib
from typing import Dict, Any, Optional, List, Tuple
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

# Force UTC for storage/compute of TimestampType values
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

# Optional: override comments without touching the struct
CHECKS_CONFIG_COMMENTS: Dict[str, str] = {
    # "check_id": "Override comment here",
}

# =========================
# StructType -> columns-spec (so TableWriter can create & document)
# =========================
def _dtype_to_name(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType): return "string"
    if isinstance(dt, T.BooleanType): return "boolean"
    if isinstance(dt, T.TimestampType): return "timestamp"
    if isinstance(dt, T.DateType): return "date"
    if isinstance(dt, T.LongType): return "long"
    if isinstance(dt, T.IntegerType): return "integer"
    if isinstance(dt, T.ShortType): return "short"
    if isinstance(dt, T.ByteType): return "byte"
    if isinstance(dt, T.FloatType): return "float"
    if isinstance(dt, T.DoubleType): return "double"
    if isinstance(dt, T.DecimalType): return f"decimal({dt.precision},{dt.scale})"
    if isinstance(dt, T.ArrayType): return "array"
    if isinstance(dt, T.MapType): return "map"
    if isinstance(dt, T.StructType): return "struct"
    # fallback
    return "string"

def _field_spec_from_struct_field(sf: T.StructField) -> Dict[str, Any]:
    dt = sf.dataType
    base: Dict[str, Any] = {
        "name": sf.name,
        "data_type": _dtype_to_name(dt),
        "nullable": bool(sf.nullable),
    }
    # comment via metadata
    meta_comment = None
    try:
        meta_comment = (sf.metadata or {}).get("comment")
    except Exception:
        meta_comment = None
    if meta_comment:
        base["comment"] = meta_comment

    if isinstance(dt, T.StructType):
        fields_spec: Dict[str, Any] = {}
        for i, child in enumerate(dt.fields, start=1):
            fields_spec[f"field_{i}"] = _field_spec_from_struct_field(child)
        base["fields"] = fields_spec

    elif isinstance(dt, T.ArrayType):
        elem = dt.elementType
        elem_spec: Dict[str, Any] = {"type": _dtype_to_name(elem)}
        if isinstance(elem, T.StructType):
            sub: Dict[str, Any] = {}
            for i, child in enumerate(elem.fields, start=1):
                sub[f"field_{i}"] = _field_spec_from_struct_field(child)
            elem_spec["fields"] = sub
        base["element"] = elem_spec

    elif isinstance(dt, T.MapType):
        base["key_type"]   = _dtype_to_name(dt.keyType)
        base["value_type"] = _dtype_to_name(dt.valueType)

    return base

def struct_to_columns_spec(struct: T.StructType, *, comments: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    cols: Dict[str, Any] = {}
    for i, sf in enumerate(struct.fields, start=1):
        spec = _field_spec_from_struct_field(sf)
        # allow comment override
        if comments and sf.name in comments:
            spec["comment"] = comments[sf.name]
        cols[f"column_{i}"] = spec
    return cols

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

    # Human display only; your tz values remain in YAML
    local_tz    = must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    print_notebook_env(spark, local_timezone=local_tz)

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
    
    
################




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
from utils.runtime import print_notebook_env
from utils.display import show_df, display_section
from utils.config import ProjectConfig, must, default_for_column, find_table
from utils.write import TableWriter, write_aligned
from utils.schema_tools import struct_to_columns_spec
from dqx.utils.dqx import (
    empty_issues_array,
    normalize_issues_for_fp,
    apply_rules_isolating_failures,
    coerce_arguments,
)

# Force UTC for compute/storage of TimestampType
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================
# SPARK STRUCTURED SCHEMAS (source of truth for targets)
# =========================

# checks_log
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

# checks_log_summary_by_rule
CHECKS_LOG_SUMMARY_BY_RULE_STRUCT = T.StructType([
    T.StructField("run_config_name",   T.StringType(), False, {"comment": "Run configuration label"}),
    T.StructField("table_name",        T.StringType(), False, {"comment": "catalog.schema.table"}),
    T.StructField("rule_name",         T.StringType(), False, {"comment": "Rule name from config"}),
    T.StructField("severity",          T.StringType(), False, {"comment": "error | warning"}),
    T.StructField("rows_flagged",      T.LongType(),   False, {"comment": "Total rows flagged by this rule"}),
    T.StructField("table_total_rows",  T.LongType(),   True,  {"comment": "Total rows evaluated for table"}),
    T.StructField("pct_of_table_rows", T.DoubleType(), False, {"comment": "rows_flagged / table_total_rows"}),
])

# checks_log_summary_by_table
CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT = T.StructType([
    T.StructField("run_config_name",          T.StringType(), False, {"comment": "Run configuration label"}),
    T.StructField("table_name",               T.StringType(), False, {"comment": "catalog.schema.table"}),
    T.StructField("table_total_rows",         T.LongType(),   False, {"comment": "count(*) evaluated"}),
    T.StructField("table_total_error_rows",   T.LongType(),   False, {"comment": "rows with non-empty _errors"}),
    T.StructField("table_total_warning_rows", T.LongType(),   False, {"comment": "rows with non-empty _warnings"}),
    T.StructField("total_flagged_rows",       T.LongType(),   False, {"comment": "rows with errors OR warnings"}),
    T.StructField("distinct_rules_fired",     T.IntegerType(),False, {"comment": "count_distinct(issue.name)"}),
])

# =========================
# Utilities for rules & projection
# =========================

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

# =========================
# Runner
# =========================
def run_checks_loader(
    spark: SparkSession,
    cfg: ProjectConfig,
    *,
    notebook_idx: int,
    exclude_cols: Optional[List[str]] = None,
    coercion_mode: str = "permissive",
) -> Dict[str, Any]:

    # Human-friendly environment print (in your local tz)
    local_tz = must(cfg.get("project_config.local_timezone"), "project_config.local_timezone")
    print_notebook_env(spark, local_timezone=local_tz)

    nb = cfg.notebook(notebook_idx)

    # Resolve checks_config (explicit override respected)
    checks_table = find_table(cfg, "checks_config")

    # Targets
    targets = nb.targets()
    t1 = targets.target_table(1)   # row log
    t2 = targets.target_table(2) if any(str(k).endswith("_2") or str(k) == "2" for k in targets.keys()) else None
    t3 = targets.target_table(3) if any(str(k).endswith("_3") or str(k) == "3" for k in targets.keys()) else None

    # Create/ensure tables from StructTypes via TableWriter
    tw = TableWriter(spark)
    apply_meta = bool(must(cfg.get("project_config.apply_table_metadata"), "project_config.apply_table_metadata"))

    # -- checks_log
    results_fqn   = t1.full_table_name()
    results_write = must(t1.get("write"),   f"{results_fqn}.write")
    results_pk    = must(t1.get("primary_key"), f"{results_fqn}.primary_key")
    results_part  = t1.get("partition_by") or []
    results_desc  = t1.get("table_description")
    results_tags  = t1.get("table_tags")

    tw.create_table(
        fqn=results_fqn,
        columns=struct_to_columns_spec(CHECKS_LOG_STRUCT),
        format=must(results_write.get("format"), f"{results_fqn}.write.format"),
        options=results_write.get("options") or {},
        partition_by=results_part,
        primary_key=results_pk,
        apply_metadata=apply_meta,
        table_comment=results_desc,
        table_tags=results_tags,
    )

    # -- summaries (optional)
    if t2:
        summary_by_rule_fqn  = t2.full_table_name()
        summary_by_rule_write= must(t2.get("write"), f"{summary_by_rule_fqn}.write")
        tw.create_table(
            fqn=summary_by_rule_fqn,
            columns=struct_to_columns_spec(CHECKS_LOG_SUMMARY_BY_RULE_STRUCT),
            format=must(summary_by_rule_write.get("format"), f"{summary_by_rule_fqn}.write.format"),
            options=summary_by_rule_write.get("options") or {},
            partition_by=t2.get("partition_by") or [],
            primary_key=must(t2.get("primary_key"), f"{summary_by_rule_fqn}.primary_key"),
            apply_metadata=apply_meta,
            table_comment=t2.get("table_description"),
            table_tags=t2.get("table_tags"),
        )
    else:
        summary_by_rule_fqn = None

    if t3:
        summary_by_table_fqn  = t3.full_table_name()
        summary_by_table_write= must(t3.get("write"), f"{summary_by_table_fqn}.write")
        tw.create_table(
            fqn=summary_by_table_fqn,
            columns=struct_to_columns_spec(CHECKS_LOG_SUMMARY_BY_TABLE_STRUCT),
            format=must(summary_by_table_write.get("format"), f"{summary_by_table_fqn}.write.format"),
            options=summary_by_table_write.get("options") or {},
            partition_by=t3.get("partition_by") or [],
            primary_key=must(t3.get("primary_key"), f"{summary_by_table_fqn}.primary_key"),
            apply_metadata=apply_meta,
            table_comment=t3.get("table_description"),
            table_tags=t3.get("table_tags"),
        )
    else:
        summary_by_table_fqn = None

    # DQ Engine
    dq = DQEngine(WorkspaceClient())

    # Try to introspect active checks count for information only
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
             .write.format(must(t2.get("write").get("format"), f"{summary_by_rule_fqn}.write.format")).mode(must(t2.get("write").get("mode"), f"{summary_by_rule_fqn}.write.mode")).options(**(t2.get("write").get("options") or {})).saveAsTable(summary_by_rule_fqn))

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

        # Align + write
        write_aligned(
            spark,
            out.select(*[f.name for f in spark.table(results_fqn).schema.fields]),
            fqn=results_fqn,
            write_block=results_write,
        )

        rows = out.count()
        grand_total += rows
        print(f"[{rc_name}] wrote {rows} rows → {results_fqn}")

        # summary by table (ALL run_configs — write once)
        if summary_by_table_fqn:
            grand_df = (
                spark.createDataFrame(all_tbl_summaries)
                .select("run_config_name","table_name","table_total_rows","table_total_error_rows","table_total_warning_rows","total_flagged_rows","distinct_rules_fired")
                .orderBy("run_config_name","table_name")
            )
            if not printed_grand_once:
                display_section("Row-hit summary by table (ALL run_configs)")
                show_df(grand_df, n=500, truncate=False)
                printed_grand_once = True

            (grand_df
             .write.format(must(t3.get("write").get("format"), f"{summary_by_table_fqn}.write.format"))
             .mode(must(t3.get("write").get("mode"), f"{summary_by_table_fqn}.write.mode"))
             .options(**(t3.get("write").get("options") or {}))
             .saveAsTable(summary_by_table_fqn))

    display_section("Grand total")
    print(f"{Color.b}{Color.ivory}TOTAL rows written: '{Color.r}{Color.b}{Color.bright_pink}{grand_total}{Color.r}{Color.b}{Color.ivory}'{Color.r}")

    return {"results_table": results_fqn, "grand_total_rows": grand_total, "checks_table": checks_table, "notebook_idx": notebook_idx}

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = ProjectConfig("resources/dqx_config.yaml")
    result = run_checks_loader(spark=spark, cfg=cfg, notebook_idx=2, coercion_mode="strict")
    print(result)
    