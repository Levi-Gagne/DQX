Give me your hoenst advice on which version of the code is better and why: 



Version 1:
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

from utils.console import Console  # <-- use Console with your Color codes

from utils.config import ProjectConfig, ConfigError, must, default_for_column
from utils.write import TableWriter, write_aligned
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
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').}}")
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
        print(f"{Console.VALIDATION} Validation only: not writing any rules.")
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
            print(f"{Console.LOADER} {full_path}: rules={len(file_rules)}")

    pre_dedupe = len(all_rules)
    rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=dedupe_mode)
    post_dedupe = len(rules)

    if not rules:
        print(f"{Console.SKIP} No rules discovered; nothing to do.")
        return {"config_path": cfg.path, "rules_files": len(yaml_files), "wrote_rows": 0, "target_table": fqn}

    print(f"{Console.DEDUPE} total parsed rules (pre-dedupe): {pre_dedupe}")
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



Version 2:
# Databricks notebook: 01_load_dqx_checks
# Purpose: Load YAML rules into dq_{env}.dqx.checks_config
# Requires: databricks-labs-dqx==0.8.x

from __future__ import annotations

import json
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

import yaml

from pyspark.sql import SparkSession, DataFrame

from databricks.labs.dqx.engine import DQEngine  # external validation

# display / utils you already have
from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.color import Color

# config & writer
from dqx.utils.config import ProjectConfig, ConfigError, must, default_for_column
from dqx.utils.write import TableWriter, write_aligned

# simple path helpers (no "resolvers")
from utils.path import dbfs_to_local, list_yaml_files

# =========================
# Column specs helpers (kept)
# =========================
def _timestamp_columns_from_spec(columns: Dict[str, Any]) -> List[str]:
    ts_cols: List[str] = []
    for spec in columns.values():
        if not isinstance(spec, dict):
            continue
        if (spec.get("data_type") or "").lower() == "timestamp":
            nm = (spec.get("name") or "").strip()
            if nm:
                ts_cols.append(nm)
    return ts_cols

def _coerce_rows_for_timestamp_fields(rows: List[dict], ts_fields: List[str]) -> None:
    """
    In-place: convert ISO strings to naive UTC datetimes for Spark TimestampType.
    Leaves None as-is.
    """
    for r in rows:
        for c in ts_fields:
            v = r.get(c, None)
            if isinstance(v, str) and v:
                try:
                    dt = datetime.fromisoformat(v)
                except Exception:
                    if v.endswith("Z"):
                        dt = datetime.fromisoformat(v[:-1] + "+00:00")
                    else:
                        raise
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                r[c] = dt

# =========================
# Canonicalization & IDs (kept)
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
# Rule YAML load/validate (kept)
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
        if not nm:
            probs.append(f"Missing rule name in {file_path}")
        if nm in seen:
            probs.append(f"Duplicate rule name '{nm}' in {file_path}")
        seen.add(nm)
    if probs:
        raise ValueError(f"File-level validation failed in {file_path}: {probs}")

def validate_rule_fields(
    rule: dict,
    file_path: str,
    required_fields: List[str],
    allowed_criticality: List[str],
):
    probs = []
    for f in required_fields:
        if not rule.get(f):
            probs.append(f"Missing required field '{f}' in rule '{rule.get('name')}' ({file_path})")
    if rule.get("table_name", "").count(".") != 2:
        probs.append(f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})")
    if rule.get("criticality") not in set(allowed_criticality):
        probs.append(f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})")
    if not rule.get("check", {}).get("function"):
        probs.append(f"Missing check.function in rule '{rule.get('name')}' ({file_path})")
    if probs:
        raise ValueError("Rule-level validation failed: " + "; ".join(probs))

def validate_with_dqx(rules: List[dict], file_path: str):
    status = DQEngine.validate_checks(rules)
    if getattr(status, "has_errors", False):
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")

# =========================
# Build rows from rule YAML docs (kept)
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
        validate_rule_fields(
            rule,
            path,
            required_fields=required_fields,
            allowed_criticality=allowed_criticality,
        )

        raw_check = rule["check"] or {}
        payload = compute_check_id_payload(rule["table_name"], raw_check, rule.get("filter"))
        check_id = compute_check_id_from_payload(payload)

        function = raw_check.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}').")

        for_each = raw_check.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').")
        if isinstance(for_each, list):
            for_each = [str(x) for x in for_each]

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
            if ZoneInfo is not None else
            datetime.now(timezone.utc).isoformat()
        )

        flat.append(
            {
                "check_id": check_id,
                "check_id_payload": payload,
                "table_name": rule["table_name"],
                "name": rule["name"],
                "criticality": rule["criticality"],
                "check": {
                    "function": function,
                    "for_each_column": for_each if for_each else None,
                    "arguments": arguments if arguments else None,
                },
                "filter": rule.get("filter"),
                "run_config_name": rule["run_config_name"],
                "user_metadata": user_metadata if user_metadata else None,
                "yaml_path": path,
                "active": rule.get("active", True),
                "created_by": created_by_value,
                "created_at": created_at_iso,
                "updated_by": None,
                "updated_at": None,
            }
        )

    validate_with_dqx(docs, path)
    return flat

# =========================
# Dedupe (on check_id) — kept
# =========================
def _fmt_rule_for_dup(r: dict) -> str:
    return (
        f"name={r.get('name')} | file={r.get('yaml_path')} | "
        f"criticality={r.get('criticality')} | run_config={r.get('run_config_name')} | "
        f"filter={r.get('filter')}"
    )

def dedupe_rules_in_batch_by_check_id(rules: List[dict], mode: str) -> List[dict]:
    groups: Dict[str, List[dict]] = {}
    for r in rules:
        groups.setdefault(r["check_id"], []).append(r)

    out: List[dict] = []
    dropped = 0
    blocks: List[str] = []

    for cid, lst in groups.items():
        if len(lst) == 1:
            out.append(lst[0]); continue
        lst = sorted(lst, key=lambda x: (x.get("yaml_path", ""), x.get("name", "")))
        keep, dups = lst[0], lst[1:]
        dropped += len(dups)
        head = f"[dup/batch/check_id] {len(dups)} duplicate(s) for check_id={cid[:12]}…"
        lines = ["    " + _fmt_rule_for_dup(x) for x in lst]
        tail = f"    -> keeping: name={keep.get('name')} | file={keep.get('yaml_path')}"
        blocks.append("\n".join([head, *lines, tail]))
        out.append(keep)

    if dropped:
        msg = "\n\n".join(blocks) + f"\n[dedupe/batch] total dropped={dropped}"
        if mode == "error":
            raise ValueError(msg)
        if mode == "warn":
            print(msg)
    return out

# =========================
# Discover rule YAMLs (kept)
# =========================
def discover_yaml(cfg: ProjectConfig, rules_dir: str) -> List[str]:
    print("[debug] rules_dir (raw from YAML):", rules_dir)
    files = list_yaml_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    df = SparkSession.builder.getOrCreate().createDataFrame([(p,) for p in files], "yaml_path string")
    show_df(df, n=500, truncate=False)
    return files

# =========================
# Build DF & alignment (kept; uses write helpers)
# =========================
def build_df_from_rules(
    spark: SparkSession,
    rules: List[dict],
    columns_spec: Dict[str, Any],
) -> DataFrame:
    ts_fields = _timestamp_columns_from_spec(columns_spec)
    if ts_fields:
        _coerce_rows_for_timestamp_fields(rules, ts_fields)
    tw = TableWriter(spark)
    schema = tw.schema_from_columns(columns_spec)
    return spark.createDataFrame(rules, schema=schema)

def preview_summary(df: DataFrame) -> None:
    display_section("SUMMARY OF RULES LOADED FROM YAML")
    totals = [(
        df.count(),
        df.select("check_id").distinct().count(),
        df.select("check_id", "run_config_name").distinct().count(),
    )]
    tdf = df.sparkSession.createDataFrame(
        totals,
        schema="`total number of rules found` long, `unique rules found` long, `distinct pair of rules` long",
    )
    show_df(tdf, n=1)

# =========================
# Write (via write_aligned)
# =========================
def write_rules(spark: SparkSession, df: DataFrame, *, fqn: str, write_block: Dict[str, Any]) -> None:
    write_aligned(spark, df, fqn=fqn, write_block=write_block)

# =========================
# Runner (config-driven)
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
    rules_dir       = must(ds.get("source_path"),          f"notebooks.notebook_{notebook_idx}.data_source.source_path")
    allowed_crit    = must(ds.get("allowed_criticality"),  f"notebooks.notebook_{notebook_idx}.data_source.allowed_criticality")
    required_fields = must(ds.get("required_fields"),      f"notebooks.notebook_{notebook_idx}.data_source.required_fields")

    t = nb.targets().target_table(1)
    fqn            = t.full_table_name()
    columns_spec   = must(t.get("columns"),        f"{fqn}.columns")
    partition_by   = t.get("partition_by") or []
    write_block    = must(t.get("write"),          f"{fqn}.write")
    table_comment  = t.get("table_description")
    table_tags     = t.get("table_tags")
    primary_key    = must(t.get("primary_key"),    f"{fqn}.primary_key")

    created_by_value = default_for_column(columns_spec, "created_by")
    must(created_by_value, f"{fqn}.columns.created_by.default_value")

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
            "distinct_rule_run_pairs": df.select("check_id", "run_config_name").distinct().count(),
            "target_table": fqn,
            "wrote_rows": 0,
            "primary_key": primary_key,
            "write_mode": must(write_block.get("mode"), f"{fqn}.write.mode"),
        }

    write_rules(spark, df.select(*[f.name for f in spark.table(fqn).schema.fields]), fqn=fqn, write_block=write_block)

    wrote_rows = df.count()
    display_section("WRITE RESULT")
    summary = spark.createDataFrame(
        [(wrote_rows, fqn, must(write_block.get("mode"), f"{fqn}.write.mode"), f"pk_{primary_key}")],
        schema="`rules written` long, `target table` string, `mode` string, `constraint` string",
    )
    show_df(summary, n=1)
    print(
        f"{Color.b}{Color.ivory}Finished writing rules to "
        f"'{Color.r}{Color.b}{Color.i}{Color.sea_green}{fqn}{Color.r}{Color.b}{Color.ivory}'{Color.r}."
    )

    return {
        "config_path": cfg.path,
        "rules_files": len(yaml_files),
        "rules_pre_dedupe": pre_dedupe,
        "rules_post_dedupe": post_dedupe,
        "unique_check_ids": df.select("check_id").distinct().count(),
        "distinct_rule_run_pairs": df.select("check_id", "run_config_name").distinct().count(),
        "target_table": fqn,
        "wrote_rows": wrote_rows,
        "primary_key": primary_key,
        "write_mode": must(write_block.get("mode"), f"{fqn}.write.mode"),
    }

# ===== Minimal entrypoint =====
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = ProjectConfig("resources/dqx_config.yaml", variables={})
    result = run_checks_loader(
        spark,
        cfg,
        notebook_idx=1,       # notebook_1: 01_load_dqx_checks
        dry_run=False,
        validate_only=False,
    )
    print(result)
