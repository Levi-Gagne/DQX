# load_dqx_checks.py

# === Cell 1: Install DQX Package (if your cluster image doesn't already include it) ===
%pip install databricks-labs-dqx

# === Cell 2: Restart Python to pick up libs (Databricks convention) ===
dbutils.library.restartPython()

# === Cell 3: Parameters (adjust paths/mode here) ===
OUTPUT_CONFIG_PATH = "resources/dqx_output_config.yaml"
RULES_DIR          = "resources/dqx_checks_config"   # trailing slash optional
TIME_ZONE          = "America/Chicago"               # for audit timestamps
MODE               = "WRITE"                         # one of: VALIDATE, DRY_RUN, WRITE

# === Cell 4: Imports & helpers ===
import os, json, re, hashlib, yaml
from typing import Dict, Any, List, Tuple

from dataclasses import dataclass
from enum import Enum, auto

from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import to_timestamp, col
from delta.tables import DeltaTable
from databricks.labs.dqx.engine import DQEngine

# your utils (kept as-is per your environment)
from utils.print import print_notebook_env
from utils.timezone import current_time_iso

# --- Section header for readable job logs ---
def section(title: str) -> None:
    bar = "═" * 70
    print(f"\n{bar}\n║ {title.ljust(68)}║\n{bar}")

# --- DQX rule table schema (adds logic_hash; hash_id == logic_hash) ---
TABLE_SCHEMA = T.StructType([
    T.StructField("hash_id",       T.StringType(), False),  # canonical content hash (same as logic_hash)
    T.StructField("logic_hash",    T.StringType(), False),  # explicit column for clarity/future
    T.StructField("table_name",    T.StringType(), False),

    # DQX fields
    T.StructField("name",          T.StringType(), False),
    T.StructField("criticality",   T.StringType(), False),
    T.StructField("check", T.StructType([
        T.StructField("function",        T.StringType(), False),
        T.StructField("for_each_column", T.ArrayType(T.StringType()), True),
        T.StructField("arguments",       T.MapType(T.StringType(), T.StringType()), True),
    ]), False),
    T.StructField("filter",         T.StringType(), True),
    T.StructField("run_config_name",T.StringType(), False),
    T.StructField("user_metadata",  T.MapType(T.StringType(), T.StringType()), True),

    # ops fields
    T.StructField("yaml_path",     T.StringType(), False),
    T.StructField("active",        T.BooleanType(), False),
    T.StructField("created_by",    T.StringType(), False),
    T.StructField("created_at",    T.StringType(), False),  # ISO string; cast on write
    T.StructField("updated_by",    T.StringType(), True),
    T.StructField("updated_at",    T.StringType(), True),
])

# --- Canonical hashing utilities ---
_WHITESPACE_RE = re.compile(r"\s+")

def _norm_ws(s: str) -> str:
    return _WHITESPACE_RE.sub(" ", s.strip())

def _sorted_list(v) -> List[str]:
    return sorted((str(x) for x in v))

def _normalize_arguments(func: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Order-insensitive, stringified, semantics-preserving normalization for check.arguments."""
    if not args:
        return {}
    func = (func or "").lower()
    out: Dict[str, Any] = {}
    for k, v in args.items():
        if v is None:
            continue
        if k in {"columns", "allowed"} and isinstance(v, (list, tuple)):
            out[k] = _sorted_list(v)
        elif k == "column":
            out[k] = str(v)
        elif k in {"min_limit", "max_limit"}:
            out[k] = str(v)
        elif k == "regex":
            out[k] = str(v)
        elif k == "expression" and isinstance(v, str):
            out[k] = _norm_ws(v)
        else:
            if isinstance(v, (list, tuple)):
                out[k] = [str(x) for x in v]
            elif isinstance(v, dict):
                out[k] = {kk: str(vv) for kk, vv in sorted(v.items())}
            else:
                out[k] = str(v)
    return out

def canonical_rule_payload(rule: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal, stable payload for hashing identity.
    NOTE: We keep for_each_column as a normalized list to match your existing behavior.
    """
    table_name = str(rule["table_name"]).lower()
    check = rule["check"]
    func = str(check["function"]).lower()
    args = _normalize_arguments(func, check.get("arguments", {}) or {})
    fec = check.get("for_each_column")
    fec_norm = _sorted_list(fec) if isinstance(fec, (list, tuple)) else None
    filt = rule.get("filter")
    filt = _norm_ws(filt) if isinstance(filt, str) else None

    # Exclude: name, run_config_name, user_metadata, criticality
    return {
        "table_name": table_name,
        "check": {"function": func, "arguments": args, "for_each_column": fec_norm},
        "filter": filt,
    }

def compute_logic_hash(rule: Dict[str, Any]) -> str:
    payload = canonical_rule_payload(rule)
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def _stringify_map_values(d: Dict[str, Any]) -> Dict[str, str]:
    """Convert arbitrary JSON-serializable values to map<string,string> for DQX."""
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

# --- Validation & processing functions (kept, with minor additions) ---
def parse_output_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    if "dqx_config_table_name" not in config or "run_config_name" not in config:
        raise ValueError("Config must include 'dqx_config_table_name' and 'run_config_name'.")
    return config

def validate_rules_file(rules, file_base: str, file_path: str):
    problems = []
    seen_names = set()
    table_names = {r.get("table_name") for r in rules if isinstance(r, dict)}
    if len(table_names) != 1:
        problems.append(f"Inconsistent table_name values in {file_path}: {table_names}")
    expected_table = file_base
    try:
        tn = list(table_names)[0]
        if tn.split(".")[-1] != expected_table:
            problems.append(f"Table name in rules ({tn}) does not match filename ({expected_table}) in {file_path}")
    except Exception:
        problems.append(f"No valid table_name found in {file_path}")
    for rule in rules:
        name = rule.get("name")
        if not name:
            problems.append(f"Missing rule name in {file_path}")
        if name in seen_names:
            problems.append(f"Duplicate rule name '{name}' in {file_path}")
        seen_names.add(name)
    if problems:
        raise ValueError(f"File-level validation failed in {file_path}: {problems}")

def validate_rule_fields(rule, file_path: str):
    problems = []
    required_fields = ["table_name", "name", "criticality", "run_config_name", "check"]
    for field in required_fields:
        if not rule.get(field):
            problems.append(f"Missing required field '{field}' in rule '{rule.get('name')}' ({file_path})")
    if rule.get("table_name", "").count(".") != 2:
        problems.append(f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})")
    if rule.get("criticality") not in {"error", "warn", "warning"}:
        problems.append(f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})")
    if not rule.get("check", {}).get("function"):
        problems.append(f"Missing check.function in rule '{rule.get('name')}' ({file_path})")
    if problems:
        raise ValueError(f"Rule-level validation failed: {problems}")

def validate_with_dqx(rules, file_path: str):
    status = DQEngine.validate_checks(rules)
    if status.has_errors:
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")

def process_yaml_file(path: str, output_config: Dict[str, Any], time_zone: str = "UTC"):
    """Read one YAML file, validate, and flatten into rows for the table."""
    file_base = os.path.splitext(os.path.basename(path))[0]
    with open(path, "r") as fh:
        docs = yaml.safe_load(fh)
    if isinstance(docs, dict):
        docs = [docs]

    validate_rules_file(docs, file_base, path)
    now = current_time_iso(time_zone)
    flat_rules = []

    for rule in docs:
        validate_rule_fields(rule, path)

        # Canonical logic hash (independent of name/run_config/criticality)
        logic_hash = compute_logic_hash(rule)

        check_dict = rule["check"]
        function = check_dict.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}').")

        for_each = check_dict.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').")
        if isinstance(for_each, list):
            try:
                for_each = [str(x) for x in for_each]
            except Exception:
                raise ValueError(f"{path}: unable to cast for_each_column items to strings (rule '{rule.get('name')}').")

        arguments = check_dict.get("arguments", {}) or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{path}: check.arguments must be a map (rule '{rule.get('name')}').")
        arguments = _stringify_map_values(arguments)

        user_metadata = rule.get("user_metadata")
        if user_metadata is not None:
            if not isinstance(user_metadata, dict):
                raise ValueError(f"{path}: user_metadata must be a map<string,string> (rule '{rule.get('name')}').")
            user_metadata = _stringify_map_values(user_metadata)
        else:
            user_metadata = {}

        # Inject rule_id into user_metadata so the runner can pick it up directly
        user_metadata.setdefault("rule_id", logic_hash)

        flat_rules.append({
            "hash_id":       logic_hash,           # CHANGED: same as logic_hash
            "logic_hash":    logic_hash,           # NEW
            "table_name":    rule["table_name"],
            "name":          rule["name"],
            "criticality":   rule["criticality"],
            "check": {
                "function":        function,
                "for_each_column": for_each if for_each else None,
                "arguments":       arguments if arguments else None,
            },
            "filter":         rule.get("filter"),
            "run_config_name":rule["run_config_name"],
            "user_metadata":  user_metadata if user_metadata else None,
            "yaml_path":      path,
            "active":         rule.get("active", True),
            "created_by":     "AdminUser",
            "created_at":     now,
            "updated_by":     None,
            "updated_at":     None,
        })

    # Semantic validation by DQX engine
    validate_with_dqx(docs, path)
    return flat_rules

def ensure_delta_table(spark: SparkSession, delta_table_name: str):
    if not spark.catalog.tableExists(delta_table_name):
        print(f"Creating new Delta table at {delta_table_name}")
        empty_df = spark.createDataFrame([], TABLE_SCHEMA)
        empty_df.write.format("delta").saveAsTable(delta_table_name)
    else:
        print(f"Delta table already exists at {delta_table_name}")

def upsert_rules_into_delta(spark: SparkSession, rules, delta_table_name: str):
    if not rules:
        print("No rules to write, skipping upsert.")
        return

    print(f"\nWriting rules to Delta table '{delta_table_name}'...")
    print(f"Number of rules to write: {len(rules)}")

    df = spark.createDataFrame(rules, schema=TABLE_SCHEMA)
    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))

    try:
        delta_table = DeltaTable.forName(spark, delta_table_name)
        delta_table.alias("target").merge(
            df.alias("source"),
            "target.yaml_path = source.yaml_path AND target.table_name = source.table_name AND target.name = source.name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        print("Delta merge failed (likely first write). Writing full table.")
        df.write.format("delta").saveAsTable(delta_table_name)

    print(f"Successfully wrote {df.count()} rules to '{delta_table_name}'.")

def print_rules_df(spark: SparkSession, rules):
    if not rules:
        print("No rules to show.")
        return
    df = spark.createDataFrame(rules, schema=TABLE_SCHEMA)
    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))
    print("\n==== Dry Run: Rules DataFrame to be uploaded ====")
    df.show(truncate=False, n=50)
    print(f"Total rules: {df.count()}")
    return df

def validate_all_rules(rules_dir: str, output_config: Dict[str, Any], fail_fast: bool = True):
    errors = []
    print(f"Starting validation for all YAML rule files in '{rules_dir}'")
    for fname in sorted(os.listdir(rules_dir)):
        if not fname.endswith((".yaml", ".yml")) or fname.startswith("."):
            continue
        full_path = os.path.join(rules_dir, fname)
        print(f"\nValidating file: {full_path}")
        try:
            file_base = os.path.splitext(os.path.basename(full_path))[0]
            with open(full_path, "r") as fh:
                docs = yaml.safe_load(fh)
            if isinstance(docs, dict):
                docs = [docs]
            validate_rules_file(docs, file_base, full_path)
            print(f"  File-level validation passed for {full_path}")
            for rule in docs:
                validate_rule_fields(rule, full_path)
                print(f"    Rule-level validation passed for rule '{rule.get('name')}'")
            validate_with_dqx(docs, full_path)
            print(f"  DQX validation PASSED for {full_path}")
        except Exception as ex:
            print(f"  Validation FAILED for file {full_path}\n  Reason: {ex}")
            errors.append(str(ex))
            if fail_fast:
                break
    if not errors:
        print("\nAll YAML rule files are valid!")
    else:
        print("\nRule validation errors found:")
        for e in errors:
            print(e)
    return errors
    
# === Cell 5: Thin Orchestrator (holds state; logic stays in pure functions) ===
class RunMode(Enum):
    VALIDATE = auto()
    DRY_RUN  = auto()
    WRITE    = auto()

@dataclass(frozen=True)
class LoaderConfig:
    output_config_path: str
    rules_dir: str
    time_zone: str = "America/Chicago"

class DQXRuleLoader:
    """Tiny facade for clear phases & logging; recomputes rules on every run."""
    def __init__(self, spark: SparkSession, cfg: LoaderConfig):
        self.spark = spark
        self.cfg = cfg

        section("PARSING OUTPUT CONFIG")
        self.output_config = parse_output_config(cfg.output_config_path)
        self.delta_table   = self.output_config["dqx_config_table_name"]

    def _load_all_rules(self) -> List[Dict[str, Any]]:
        section("LOADING + FLATTENING RULES FROM YAML")
        rules: List[Dict[str, Any]] = []
        rules_dir = self.cfg.rules_dir.rstrip("/")

        for fname in sorted(os.listdir(rules_dir)):
            if not fname.endswith((".yaml", ".yml")) or fname.startswith("."):
                continue
            path = os.path.join(rules_dir, fname)
            rules.extend(process_yaml_file(path, self.output_config, time_zone=self.cfg.time_zone))
        print(f"Collected {len(rules)} rule rows from YAML.")
        return rules

    def run(self, mode: RunMode) -> None:
        section("ENVIRONMENT")
        print_notebook_env(self.spark, local_timezone=self.cfg.time_zone)

        rules = self._load_all_rules()

        if mode is RunMode.VALIDATE:
            section("VALIDATE-ONLY")
            validate_all_rules(self.cfg.rules_dir, self.output_config)
            return

        if mode is RunMode.DRY_RUN:
            section("DRY RUN (SHOW DATAFRAME)")
            print_rules_df(self.spark, rules)
            return

        section("WRITE (UPSERT INTO DELTA)")
        ensure_delta_table(self.spark, self.delta_table)
        upsert_rules_into_delta(self.spark, rules, self.delta_table)
        print(f"Finished writing rules to '{self.delta_table}'.")
        
        
# === Cell 6: Entrypoint ===
spark = SparkSession.builder.getOrCreate()

loader = DQXRuleLoader(
    spark,
    LoaderConfig(
        output_config_path=OUTPUT_CONFIG_PATH,
        rules_dir=RULES_DIR,
        time_zone=TIME_ZONE,
    ),
)

mode = {
    "VALIDATE": RunMode.VALIDATE,
    "DRY_RUN":  RunMode.DRY_RUN,
    "WRITE":    RunMode.WRITE,
}.get(MODE.upper(), RunMode.WRITE)

loader.run(mode)


