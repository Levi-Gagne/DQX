import json
import hashlib
import yaml
from typing import Dict, Any, Optional, List, Tuple

from pyspark.sql import SparkSession, DataFrame, types as T
from pyspark.sql.functions import to_timestamp, col, desc

from databricks.labs.dqx.engine import DQEngine

# UPDATED: single display util + plain-text runtime banner
from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.timer import current_time_iso
from utils.color import Color
from utils.config import ProjectConfig  # uses utils.path under the hood

# Notebook-safe __file__ guard (Databricks notebooks don't define __file__)
try:
    THIS_FILE = __file__
except NameError:
    THIS_FILE = None


# ======================================================
# DQX argument spec (row + dataset checks)
# ======================================================
_EXPECTED: Dict[str, Dict[str, str]] = {
    # ---------- Row-level ----------
    "is_not_null": {"column": "str"},
    "is_not_empty": {"column": "str"},
    "is_not_null_and_not_empty": {"column": "str", "trim_strings": "bool"},
    "is_in_list": {"column": "str", "allowed": "list"},
    "is_not_null_and_is_in_list": {"column": "str", "allowed": "list"},
    "is_not_null_and_not_empty_array": {"column": "str"},

    "is_in_range": {
        "column": "str",
        "min_limit": "num",
        "max_limit": "num",
        "inclusive_min": "bool",
        "inclusive_max": "bool",
    },
    "is_not_in_range": {
        "column": "str",
        "min_limit": "num",
        "max_limit": "num",
        "inclusive_min": "bool",
        "inclusive_max": "bool",
    },
    "is_not_less_than": {"column": "str", "limit": "num"},
    "is_not_greater_than": {"column": "str", "limit": "num"},

    "is_valid_date": {"column": "str", "date_format": "str"},
    "is_valid_timestamp": {"column": "str", "timestamp_format": "str"},

    "is_not_in_future": {"column": "str", "offset": "num", "curr_timestamp": "str"},
    "is_not_in_near_future": {"column": "str", "offset": "num", "curr_timestamp": "str"},

    "is_older_than_n_days": {
        "column": "str",
        "days": "num",
        "curr_date": "str",
        "negate": "bool",
    },
    "is_older_than_col2_for_n_days": {
        "column1": "str",
        "column2": "str",
        "days": "num",
        "negate": "bool",
    },

    "regex_match": {"column": "str", "regex": "str", "negate": "bool"},

    "sql_expression": {
        "expression": "str",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "columns": "list",
    },

    "is_valid_ipv4_address": {"column": "str"},
    "is_ipv4_address_in_cidr": {"column": "str", "cidr_block": "str"},

    "is_data_fresh": {
        "column": "str",
        "max_age_minutes": "num",
        "base_timestamp": "str",
    },

    # ---------- Dataset-level ----------
    "is_unique": {"columns": "list", "nulls_distinct": "bool"},
    "is_aggr_not_greater_than": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_not_less_than": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_equal": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_not_equal": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },

    "foreign_key": {
        "columns": "list",
        "ref_columns": "list",
        "ref_df_name": "str",
        "ref_table": "str",
        "negate": "bool",
    },

    "sql_query": {
        "query": "str",
        "merge_columns": "list",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "condition_column": "str",
        "input_placeholder": "str",
        "row_filter": "str",
    },

    "compare_datasets": {
        "columns": "list",
        "ref_columns": "list",
        "exclude_columns": "list",
        "ref_df_name": "str",
        "ref_table": "str",
        "check_missing_records": "bool",
        "null_safe_row_matching": "bool",
        "null_safe_column_value_matching": "bool",
    },

    "is_data_fresh_per_time_window": {
        "column": "str",
        "window_minutes": "num",
        "min_records_per_window": "num",
        "lookback_windows": "num",
        "curr_timestamp": "str",
    },
}

# =========================
# Target schema (Delta sink)
# =========================
TABLE_SCHEMA = T.StructType([
    T.StructField("check_id",            T.StringType(), False),  # PRIMARY KEY (by convention + runtime assertion)
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
    T.StructField("created_at",          T.StringType(), False),  # ISO string; cast on write
    T.StructField("updated_by",          T.StringType(), True),
    T.StructField("updated_at",          T.StringType(), True),
])

# ======================================================
# Documentation dictionary
# ======================================================
DQX_CHECKS_CONFIG_METADATA: Dict[str, Any] = {
    "table": "<override at create time>",
    "table_comment": (
        "## DQX Checks Configuration\n"
        "- One row per **unique canonical rule** generated from YAML (source of truth).\n"
        "- **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.\n"
        "- Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.\n"
        "- Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.\n"
        "- `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.\n"
        "- `run_config_name` is a **routing tag**, not part of identity.\n"
        "- Only rows with `active=true` are executed."
    ),
    "columns": {
        "check_id": "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}.",
        "check_id_payload": "Canonical JSON used to derive `check_id` (sorted keys, normalized values).",
        "table_name": "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability.",
        "name": "Human-readable rule name. Used in UI/diagnostics and name-based joins when enriching logs.",
        "criticality": "Rule severity: `warn|warning|error`. Reporting normalizes warn/warning → `warning`.",
        "check": "Structured rule: `{function, for_each_column?, arguments?}`; argument values stringified.",
        "filter": "Optional SQL predicate applied before evaluation (row-level). Normalized in payload.",
        "run_config_name": "Execution group/tag. Drives which runs pick up this rule; **not** part of identity.",
        "user_metadata": "Free-form `map<string,string>` carried through to issues for traceability.",
        "yaml_path": "Absolute/volume path to the defining YAML doc (lineage).",
        "active": "If `false`, rule is ignored by runners.",
        "created_by": "Audit: creator/principal that materialized the row.",
        "created_at": "Audit: creation timestamp (cast to TIMESTAMP on write).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (nullable; cast to TIMESTAMP on write).",
    },
}

def _materialize_table_doc(doc_template: Dict[str, Any], table_fqn: str) -> Dict[str, Any]:
    copy = json.loads(json.dumps(doc_template))
    copy["table"] = table_fqn
    if "table_comment" in copy and isinstance(copy["table_comment"], str):
        copy["table_comment"] = copy["table_comment"].replace("{TABLE_FQN}", table_fqn)
    return copy

# =========================
# YAML loading (robust)
# =========================
def load_yaml_rules(path: str) -> List[dict]:
    with open(path, "r") as fh:
        docs = list(yaml.safe_load_all(fh))
    out: List[dict] = []
    for d in docs:
        if d is None:
            continue
        if isinstance(d, dict):
            out.append(d)
        elif isinstance(d, list):
            out.extend([x for x in d if isinstance(x, dict)])
    return out

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
    allowed_criticality={"error", "warn", "warning"},
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

# Safe time helper: fall back to UTC if zoneinfo isn't available on the host
def _now_iso_safe(tz: str) -> str:
    try:
        return current_time_iso(tz)
    except Exception:
        return current_time_iso("UTC")

# =========================
# Build rows
# =========================
def process_yaml_file(path: str, required_fields: List[str], time_zone: str = "UTC", created_by: str = "AdminUser") -> List[dict]:
    docs = load_yaml_rules(path)
    if not docs:
        print(f"[skip] {path} has no rules.")
        return []

    validate_rules_file(docs, path)

    # Use timezone if present, otherwise quietly fall back to UTC
    now = _now_iso_safe(time_zone)
    flat: List[dict] = []

    for rule in docs:
        validate_rule_fields(rule, path, required_fields=required_fields)

        raw_check = rule["check"] or {}
        payload = compute_check_id_payload(rule["table_name"], raw_check, rule.get("filter"))
        check_id = compute_check_id_from_payload(payload)

        function = raw_check.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(
                f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}')."
            )

        for_each = raw_check.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(
                f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}')."
            )
        if isinstance(for_each, list):
            try:
                for_each = [str(x) for x in for_each]
            except Exception:
                raise ValueError(
                    f"{path}: unable to cast for_each_column items to strings (rule '{rule.get('name')}')."
                )

        arguments = raw_check.get("arguments", {}) or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{path}: check.arguments must be a map (rule '{rule.get('name')}').")
        arguments = _stringify_map_values(arguments)

        user_metadata = rule.get("user_metadata")
        if user_metadata is not None:
            if not isinstance(user_metadata, dict):
                raise ValueError(f"{path}: user_metadata must be a map (rule '{rule.get('name')}').")
            user_metadata = _stringify_map_values(user_metadata)

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
                "created_at": now,
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
# Comments + Constraints + Helpers
# =========================
def _esc_comment(s: str) -> str:
    return (s or "").replace("'", "''")

def _q_fqn(fqn: str) -> str:
    return ".".join(f"`{p}`" for p in fqn.split("."))

def _split_fqn(fqn: str) -> Tuple[str, str, str]:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got '{fqn}'")
    return parts[0], parts[1], parts[2]

def preview_table_documentation(spark: SparkSession, table_fqn: str, doc: Dict[str, Any]) -> None:
    display_section("TABLE METADATA PREVIEW (markdown text stored in comments)")
    doc_df = spark.createDataFrame(
        [(table_fqn, doc.get("table_comment", ""))],
        schema="table string, table_comment_markdown string",
    )
    show_df(doc_df, n=1, truncate=False)

    cols = doc.get("columns", {}) or {}
    cols_df = spark.createDataFrame(
        [(k, v) for k, v in cols.items()],
        schema="column string, column_comment_markdown string",
    )
    show_df(cols_df, n=200, truncate=False)

def apply_table_documentation(
    spark: SparkSession,
    table_fqn: str,
    doc: Optional[Dict[str, Any]],
) -> None:
    if not doc:
        return
    qtable = _q_fqn(table_fqn)

    # Table comment
    table_comment = _esc_comment(doc.get("table_comment", ""))
    if table_comment:
        try:
            spark.sql(f"COMMENT ON TABLE {qtable} IS '{table_comment}'")
        except Exception:
            spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    # Column comments (always attempt)
    cols: Dict[str, str] = doc.get("columns", {}) or {}
    existing_cols = {f.name.lower() for f in spark.table(table_fqn).schema.fields}
    for col_name, comment in cols.items():
        if col_name.lower() not in existing_cols:
            continue
        qcol = f"`{col_name}`"
        comment_sql = f"ALTER TABLE {qtable} ALTER COLUMN {qcol} COMMENT '{_esc_comment(comment)}'"
        try:
            spark.sql(comment_sql)
        except Exception:
            # Try COMMENT ON COLUMN as fallback
            try:
                spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{_esc_comment(comment)}'")
            except Exception as e2:
                print(f"[meta] Skipped column comment for {table_fqn}.{col_name}: {e2}")

def _set_not_null_if_possible(spark: SparkSession, table_fqn: str, column: str) -> None:
    """Ensure the PK column is NOT NULL (required by UC for PRIMARY KEY)."""
    qtable = _q_fqn(table_fqn)
    try:
        spark.sql(f"ALTER TABLE {qtable} ALTER COLUMN `{column}` SET NOT NULL")
    except Exception:
        pass

def _constraints_df(spark: SparkSession, table_fqn: str) -> DataFrame:
    """(Optional) Query information_schema to see the PK listed."""
    cat, sch, tbl = _split_fqn(table_fqn)
    return spark.sql(f"""
        SELECT tc.constraint_name,
               tc.constraint_type,
               kcu.column_name
        FROM system.information_schema.table_constraints tc
        LEFT JOIN system.information_schema.key_column_usage kcu
          ON tc.table_catalog = kcu.table_catalog
         AND tc.table_schema  = kcu.table_schema
         AND tc.table_name    = kcu.table_name
         AND tc.constraint_name = kcu.constraint_name
        WHERE tc.table_catalog = '{cat}'
          AND tc.table_schema  = '{sch}'
          AND tc.table_name    = '{tbl}'
    """)

# =========================
# Delta I/O
#   * On first-ever create: create empty table, add PK once, then append data.
#   * On subsequent runs   : simple overwrite of data (no PK changes).
# =========================
def ensure_schema_exists(spark: SparkSession, full_table_name: str):
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a 3-part name (catalog.schema.table), got '{full_table_name}'")
    cat, sch, _ = parts
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")

def overwrite_rules_into_delta(
    spark: SparkSession,
    df: DataFrame,
    delta_table_name: str,
    table_doc: Optional[Dict[str, Any]] = None,
    *,
    primary_key: str = "check_id",
):
    """
    Overwrite table data on every run.
    Add PRIMARY KEY **only on initial creation** (never on subsequent runs).
    """
    ensure_schema_exists(spark, delta_table_name)

    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))

    qtable = _q_fqn(delta_table_name)
    existed_before = spark.catalog.tableExists(delta_table_name)

    if not existed_before:
        # 1) Create empty table with the exact schema
        empty_df = df.limit(0)
        empty_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(delta_table_name)

        # 2) Add PK (once)
        try:
            _set_not_null_if_possible(spark, delta_table_name, primary_key)
            spark.sql(f"ALTER TABLE {qtable} ADD CONSTRAINT pk_{primary_key} PRIMARY KEY (`{primary_key}`) RELY")
        except Exception as e:
            print(f"[meta] Could not set PRIMARY KEY on first create for {delta_table_name}.{primary_key}: {e}")

        # 3) Append the actual data (table is empty; append == overwrite)
        df.write.format("delta").mode("append").saveAsTable(delta_table_name)
    else:
        # Normal overwrite of data; do NOT touch PK
        df.write.format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(delta_table_name)

    # Apply comments (safe to re-apply; no effect on PK)
    doc_to_apply = _materialize_table_doc(table_doc or DQX_CHECKS_CONFIG_METADATA, delta_table_name)
    apply_table_documentation(spark, delta_table_name, doc_to_apply)

    # (Optional) Peek constraints so you can confirm PK is present
    try:
        display_section("CONSTRAINTS (information_schema)")
        cdf = _constraints_df(spark, delta_table_name)
        show_df(cdf.orderBy("constraint_type", "constraint_name"), n=100, truncate=False)
    except Exception as e:
        print(f"[meta] Could not read information_schema constraints: {e}")

    # Preview metadata
    preview_table_documentation(spark, delta_table_name, doc_to_apply)

    display_section("WRITE RESULT (Delta)")
    summary = spark.createDataFrame(
        [(df.count(), delta_table_name, "overwrite", f"pk_{primary_key}")],
        schema="`rules written` long, `target table` string, `mode` string, `constraint` string",
    )
    show_df(summary, n=1)
    print(f"\n{Color.b}{Color.ivory}Rules written to: '{Color.r}{Color.b}{Color.chartreuse}{delta_table_name}{Color.r}{Color.b}{Color.ivory}'  (PK set on first create only)")

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
    sample_cols = df_rules.select("check_id", "name", "run_config_name", "yaml_path").orderBy(
        desc("yaml_path")
    )
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
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False,
    required_fields: Optional[List[str]] = None,
    batch_dedupe_mode: str = "warn",  # warn | error | skip
    table_doc: Optional[Dict[str, Any]] = None,
    created_by: str = "AdminUser",
):
    spark = SparkSession.builder.getOrCreate()

    # Use requested TZ if available, otherwise print runtime banner in UTC
    safe_tz = time_zone
    try:
        _ = current_time_iso(time_zone)
    except Exception:
        safe_tz = "UTC"
    print_notebook_env(spark, local_timezone=safe_tz)

    cfg = ProjectConfig(output_config_path, spark=spark, start_file=THIS_FILE)
    rules_dir = rules_dir or cfg.yaml_rules_dir()
    delta_table_name, primary_key = cfg.checks_config_table()

    required_fields = required_fields or ["table_name", "name", "criticality", "run_config_name", "check"]

    # Recursively discover YAML files via config helper
    yaml_files = cfg.list_rule_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    files_df = spark.createDataFrame([(p,) for p in yaml_files], "yaml_path string")
    show_df(files_df, n=500, truncate=False)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        errs = validate_rule_files(yaml_files, required_fields)
        return {
            "mode": "validate_only",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "errors": errs,
        }

    # Collect rules
    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(full_path, required_fields=required_fields, time_zone=time_zone, created_by=created_by)
        if file_rules:
            all_rules.extend(file_rules)
            print(f"[loader] {full_path}: rules={len(file_rules)}")

    if not all_rules:
        print("No rules discovered; nothing to do.")
        return {"mode": "no_op", "config_path": output_config_path, "rules_files": len(yaml_files), "wrote_rows": 0}

    print(f"[loader] total parsed rules (pre-dedupe): {len(all_rules)}")

    # In-batch dedupe on check_id only
    pre_dedupe = len(all_rules)
    all_rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=batch_dedupe_mode)
    post_dedupe = len(all_rules)

    # Assemble DataFrame and DISPLAY diagnostics
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

    # OVERWRITE data (PK added only on first create)
    doc = _materialize_table_doc(table_doc or DQX_CHECKS_CONFIG_METADATA, delta_table_name)
    overwrite_rules_into_delta(spark, df, delta_table_name, table_doc=doc, primary_key=primary_key)
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
    created_by: str = "AdminUser",
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False,
    batch_dedupe_mode: str = "warn",
    table_doc: Optional[Dict[str, Any]] = None,
):
    """
    Notebook-friendly entrypoint mirroring 02_run_dqx_checks.run_checks(...).
    """
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
    )

# ---- run it ----
res = load_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # dry_run=True,
    # validate_only=True,
    batch_dedupe_mode="warn",
)
print(res)