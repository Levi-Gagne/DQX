import json
import hashlib
from typing import Dict, Any, Optional, List

from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

from pyspark.sql import SparkSession, DataFrame, types as T
from pyspark.sql.functions import to_timestamp, col, desc

from databricks.labs.dqx.engine import DQEngine

from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.color import Color
from utils.config import ProjectConfig
from utils.table import (
    table_exists,
    add_primary_key_constraint,
    write_empty_delta_table,
    empty_df_from_schema,
    create_table_if_absent,
)
from utils.documentation import (
    _materialize_table_doc,
    _q_fqn,
    _esc_comment,
    preview_table_documentation,
    apply_table_documentation,
    doc_from_config,
)

from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED

try:
    THIS_FILE = __file__
except NameError:
    THIS_FILE = None

# =========================
# Target schema (Delta sink)
# =========================
TABLE_SCHEMA = T.StructType([
    T.StructField("check_id",            T.StringType(), False),
    T.StructField("check_id_payload",    T.StringType(), False),
    T.StructField("table_name",          T.StringType(), False),

    # DQX fields
    T.StructField("name",                T.StringType(), False),
    T.StructField("criticality",         T.StringType(), False),
    T.StructField("check", T.StructType([
        T.StructField("function",        T.StringType(), False),
        T.StructField("for_each_column", T.ArrayType(T.StringType()), True),
        T.StructField("arguments",       T.MapType(T.StringType(), T.StringType()), True),
    ]), False),
    T.StructField("filter",              T.StringType(), True),
    T.StructField("run_config_name",     T.StringType(), False),
    T.StructField("user_metadata",       T.MapType(T.StringType(), T.StringType()), True),

    # Ops fields
    T.StructField("yaml_path",           T.StringType(), False),
    T.StructField("active",              T.BooleanType(), False),
    T.StructField("created_by",          T.StringType(), False),
    T.StructField("created_at",          T.StringType(), False),
    T.StructField("updated_by",          T.StringType(), True),
    T.StructField("updated_at",          T.StringType(), True),
])

# =========================
# Canonicalization & IDs
# =========================
def _canon_filter(s: Optional[str]) -> str:
    return "" if not s else " ".join(str(s).split())

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
# Conversions / validation
# =========================
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
    allowed_criticality={"error", "warn"},
):
    probs = []
    for f in required_fields:
        if not rule.get(f):
            probs.append(f"Missing required field '{f}' in rule '{rule.get('name')}' ({file_path})")
    if rule.get("table_name", "").count(".") != 2:
        probs.append(
            f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})"
        )
    if rule.get("criticality") not in allowed_criticality:
        probs.append(
            f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})"
        )
    if not rule.get("check", {}).get("function"):
        probs.append(f"Missing check.function in rule '{rule.get('name')}' ({file_path})")
    if probs:
        raise ValueError("Rule-level validation failed: " + "; ".join(probs))

def validate_with_dqx(rules: List[dict], file_path: str):
    status = DQEngine.validate_checks(rules)
    if getattr(status, "has_errors", False):
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")

# =========================
# Build rows
# =========================
def process_yaml_file(
    path: str,
    required_fields: List[str],
    time_zone: str = "UTC",
    created_by: str = "AdminUser",
    allowed_criticality: Optional[List[str]] = None,
) -> List[dict]:
    docs = ProjectConfig.load_yaml_rules(path)
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
            allowed_criticality=set(allowed_criticality or {"error", "warn"}),
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
            try:
                for_each = [str(x) for x in for_each]
            except Exception:
                raise ValueError(f"{path}: unable to cast for_each_column items to strings (rule '{rule.get('name')}').")

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
                "created_by": created_by,
                "created_at": created_at_iso,
                "updated_by": None,
                "updated_at": None,
            }
        )

    validate_with_dqx(docs, path)
    return flat

# =========================
# Batch dedupe (on check_id ONLY)
# =========================
def _fmt_rule_for_dup(r: dict) -> str:
    return (
        f"name={r.get('name')} | file={r.get('yaml_path')} | "
        f"criticality={r.get('criticality')} | run_config={r.get('run_config_name')} | "
        f"filter={r.get('filter')}"
    )

def dedupe_rules_in_batch_by_check_id(rules: List[dict], mode: str = "warn") -> List[dict]:
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
# Data overwrite (preserve metadata)
# =========================
def overwrite_rules_into_delta(
    spark: SparkSession,
    df: DataFrame,
    delta_table_name: str,
    table_doc: Optional[Dict[str, Any]] = None,
    *,
    primary_key: str = "check_id",
):
    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))

    if not table_exists(spark, delta_table_name):
        create_table_if_absent(
            spark, delta_table_name, schema=TABLE_SCHEMA,
            table_doc=table_doc,
            primary_key=primary_key
        )

    target_schema = spark.table(delta_table_name).schema
    target_cols = [f.name for f in target_schema.fields]
    missing = [c for c in target_cols if c not in df.columns]
    extra   = [c for c in df.columns if c not in target_cols]
    if missing or extra:
        raise ValueError(
            f"Schema mismatch for INSERT OVERWRITE.\n"
            f"Missing in df: {missing}\nExtra in df: {extra}"
        )

    df_ordered = df.select(*target_cols)
    tmp_view = "__tmp_overwrite_rules"
    df_ordered.createOrReplaceTempView(tmp_view)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {_q_fqn(delta_table_name)} SELECT * FROM {tmp_view}")
    finally:
        try:
            spark.catalog.dropTempView(tmp_view)
        except Exception:
            pass

    display_section("WRITE RESULT (Delta)")
    summary = spark.createDataFrame(
        [(df.count(), delta_table_name, "insert overwrite", f"pk_{primary_key}")],
        schema="`rules written` long, `target table` string, `mode` string, `constraint` string",
    )
    show_df(summary, n=1)
    print(
        f"\n{Color.b}{Color.ivory}Rules written to: "
        f"'{Color.r}{Color.b}{Color.chartreuse}{delta_table_name}{Color.r}{Color.b}{Color.ivory}' "
        f"(metadata preserved){Color.r}"
    )

# =========================
# Display-first debug helpers
# =========================
def debug_display_batch(spark: SparkSession, df_rules: DataFrame) -> None:
    display_section("SUMMARY OF RULES LOADED FROM YAML")
    totals = [
        (
            df_rules.count(),
            df_rules.select("check_id").distinct().count(),
            df_rules.select("check_id", "run_config_name").distinct().count(),
        )
    ]
    totals_df = spark.createDataFrame(
        totals,
        schema="`total number of rules found` long, `unique rules found` long, `distinct pair of rules` long",
    )
    show_df(totals_df, n=1)

    display_section("SAMPLE OF RULES LOADED FROM YAML (check_id, name, run_config_name, yaml_path)")
    sample_cols = df_rules.select("check_id", "name", "run_config_name", "yaml_path").orderBy(desc("yaml_path"))
    show_df(sample_cols, n=50, truncate=False)

    display_section("RULES LOADED PER TABLE")
    by_table = df_rules.groupBy("table_name").count().orderBy(desc("count"))
    show_df(by_table, n=200)

def print_rules_df(spark: SparkSession, rules: List[dict]) -> Optional[DataFrame]:
    if not rules:
        print("No rules to show.")
        return None
    df = (
        spark.createDataFrame(rules, schema=TABLE_SCHEMA)
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("updated_at", to_timestamp(col("updated_at")))
    )
    debug_display_batch(spark, df)
    return df

# =========================
# Main
# =========================
def main(
    output_config_path: str = "resources/dqx_config.yaml",
    rules_dir: Optional[str] = None,
    time_zone: Optional[str] = None,
    dry_run: bool = False,
    validate_only: bool = False,
    required_fields: Optional[List[str]] = None,
    batch_dedupe_mode: Optional[str] = None,
    table_doc: Optional[Dict[str, Any]] = None,
    created_by: Optional[str] = None,
    apply_table_metadata: Optional[bool] = None,
):
    spark = SparkSession.builder.getOrCreate()

    cfg = ProjectConfig(output_config_path, spark=spark)
    time_zone = time_zone or cfg.local_timezone()
    print_notebook_env(spark, local_timezone=time_zone)

    rules_dir = rules_dir or cfg.yaml_rules_dir()
    delta_table_name, primary_key = cfg.checks_config_table()

    required_fields = required_fields or cfg.required_fields()
    allowed_crit = cfg.allowed_criticality()
    created_by = created_by or cfg.created_by()
    batch_dedupe_mode = batch_dedupe_mode or cfg.batch_dedupe_mode()

    # Create-once / metadata gating
    doc_tpl = table_doc or cfg.table_doc("checks_config_table")
    doc = _materialize_table_doc(doc_tpl, delta_table_name) if doc_tpl else None

    exists = table_exists(spark, delta_table_name)
    if not exists:
        create_table_if_absent(
            spark,
            delta_table_name,
            schema=TABLE_SCHEMA,
            table_doc=doc,
            primary_key=primary_key,
            partition_by=None,
        )
    elif (cfg.apply_table_metadata_flag() if apply_table_metadata is None else apply_table_metadata):
        apply_table_documentation(spark, delta_table_name, doc)

    # Discover YAML files
    yaml_files = cfg.list_rule_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    files_df = spark.createDataFrame([(p,) for p in yaml_files], "yaml_path string")
    show_df(files_df, n=500, truncate=False)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        errs: List[str] = []
        for p in yaml_files:
            try:
                validate_rules_file(ProjectConfig.load_yaml_rules(p), p)
            except Exception as e:
                errs.append(f"{p}: {e}")
        return {
            "mode": "validate_only",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "errors": errs,
        }

    # Collect rules
    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(
            full_path,
            required_fields=required_fields,
            time_zone=time_zone,
            created_by=created_by,
            allowed_criticality=allowed_crit,
        )
        if file_rules:
            all_rules.extend(file_rules)
            print(f"[loader] {full_path}: rules={len(file_rules)}")

    if not all_rules:
        print("No rules discovered; nothing to do.")
        return {"mode": "no_op", "config_path": output_config_path, "rules_files": len(yaml_files), "wrote_rows": 0}

    print(f"[loader] total parsed rules (pre-dedupe): {len(all_rules)}")

    pre_dedupe = len(all_rules)
    all_rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=batch_dedupe_mode)
    post_dedupe = len(all_rules)

    df = spark.createDataFrame(all_rules, schema=TABLE_SCHEMA)
    debug_display_batch(spark, df)

    unique_check_ids = df.select("check_id").distinct().count()
    distinct_pairs  = df.select("check_id", "run_config_name").distinct().count()

    if dry_run:
        display_section("DRY-RUN: FULL RULES PREVIEW")
        show_df(df.orderBy("table_name", "name"), n=1000, truncate=False)
        return {
            "mode": "dry_run",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "rules_pre_dedupe": pre_dedupe,
            "rules_post_dedupe": post_dedupe,
            "unique_check_ids": unique_check_ids,
            "distinct_rule_run_pairs": distinct_pairs,
            "target_table": delta_table_name,
            "wrote_rows": 0,
            "primary_key": primary_key,
        }

    overwrite_rules_into_delta(
        spark,
        df,
        delta_table_name,
        table_doc=doc,
        primary_key=primary_key,
    )
    wrote_rows = df.count()
    print(f"{Color.b}{Color.ivory}Finished writing rules to '{Color.r}{Color.b}{Color.i}{Color.sea_green}{delta_table_name}{Color.r}{Color.b}{Color.ivory}' (overwrite){Color.r}.")

    return {
        "mode": "overwrite",
        "config_path": output_config_path,
        "rules_files": len(yaml_files),
        "rules_pre_dedupe": pre_dedupe,
        "rules_post_dedupe": post_dedupe,
        "unique_check_ids": unique_check_ids,
        "distinct_rule_run_pairs": distinct_pairs,
        "target_table": delta_table_name,
        "wrote_rows": wrote_rows,
        "constraint": f"pk_{primary_key}",
        "primary_key": primary_key,
    }

def load_checks(
    dqx_cfg_yaml: str = "resources/dqx_config.yaml",
    time_zone: Optional[str] = None,
    dry_run: bool = False,
    validate_only: bool = False,
    batch_dedupe_mode: Optional[str] = None,
    table_doc: Optional[Dict[str, Any]] = None,
    created_by: Optional[str] = None,
    apply_table_metadata: Optional[bool] = None,
):
    return main(
        output_config_path=dqx_cfg_yaml,
        rules_dir=None,
        time_zone=time_zone,
        dry_run=dry_run,
        validate_only=validate_only,
        required_fields=None,
        batch_dedupe_mode=batch_dedupe_mode,
        table_doc=table_doc,
        created_by=created_by,
        apply_table_metadata=apply_table_metadata,
    )

# ---- run it ----
res = load_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    # overrides are optional; omit to use YAML-configured values
)
print(res)