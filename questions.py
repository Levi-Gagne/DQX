I know we updated the code to load checks, but i want to focus on the version of the code i have belwo:

import os
import json
import hashlib
import yaml
from typing import Dict, Any

from pyspark.sql import SparkSession, types as T
from delta.tables import DeltaTable
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql.functions import to_timestamp, col

from utils.print import print_notebook_env
from utils.timezone import current_time_iso


# --- Unified schema, with DQX columns sandwiched ---
# DQX core columns must match the docs:
# name (STRING), criticality (STRING), check (STRUCT), filter (STRING),
# run_config_name (STRING), user_metadata (MAP<STRING,STRING>)
TABLE_SCHEMA = T.StructType([
    T.StructField("hash_id", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),

    # DQX fields begin here
    T.StructField("name", T.StringType(), False),
    T.StructField("criticality", T.StringType(), False),
    T.StructField(
        "check",
        T.StructType([
            T.StructField("function", T.StringType(), False),
            T.StructField("for_each_column", T.ArrayType(T.StringType()), True),
            T.StructField("arguments", T.MapType(T.StringType(), T.StringType()), True),
        ]),
        False,
    ),
    T.StructField("filter", T.StringType(), True),
    T.StructField("run_config_name", T.StringType(), False),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),

    # your ops fields
    T.StructField("yaml_path", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("created_by", T.StringType(), False),
    T.StructField("created_at", T.StringType(), False),  # stored as ISO string; we may cast on write
    T.StructField("updated_by", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True),
])


def compute_hash(rule_dict: Dict[str, Any]) -> str:
    """Stable hash over the identifying fields of a rule."""
    relevant = {
        k: rule_dict[k]
        for k in ["table_name", "name", "criticality", "run_config_name", "check"]
        if k in rule_dict
    }
    return hashlib.md5(json.dumps(relevant, sort_keys=True).encode()).hexdigest()


def _stringify_map_values(d: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert a dict of arbitrary JSON-serializable values to map<string,string>
    required by DQX (lists/dicts -> JSON, bool -> 'true'/'false', else str()).
    """
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

        h = compute_hash(rule)
        check_dict = rule["check"]

        # Strong typing for DQX struct:
        function = check_dict.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}').")

        for_each = check_dict.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').")
        if isinstance(for_each, list):
            # ensure all entries are strings (DQX schema uses ARRAY<STRING>)
            try:
                for_each = [str(x) for x in for_each]
            except Exception:
                raise ValueError(f"{path}: unable to cast for_each_column items to strings (rule '{rule.get('name')}').")

        arguments = check_dict.get("arguments", {}) or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{path}: check.arguments must be a map (rule '{rule.get('name')}').")
        arguments = _stringify_map_values(arguments)  # enforce map<string,string>

        user_metadata = rule.get("user_metadata")
        if user_metadata is not None:
            if not isinstance(user_metadata, dict):
                raise ValueError(f"{path}: user_metadata must be a map<string,string> (rule '{rule.get('name')}').")
            user_metadata = _stringify_map_values(user_metadata)

        flat_rules.append({
            "hash_id": h,
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
            "created_by": "AdminUser",
            "created_at": now,
            "updated_by": None,
            "updated_at": None,
        })

    # Validate with DQX engine (semantic)
    validate_with_dqx(docs, path)
    return flat_rules


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
            problems.append(
                f"Table name in rules ({tn}) does not match filename ({expected_table}) in {file_path}"
            )
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
            problems.append(
                f"Missing required field '{field}' in rule '{rule.get('name')}' ({file_path})"
            )
    if rule.get("table_name", "").count(".") != 2:
        problems.append(
            f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})"
        )
    if rule.get("criticality") not in {"error", "warn", "warning"}:
        problems.append(
            f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})"
        )
    if not rule.get("check", {}).get("function"):
        problems.append(
            f"Missing check.function in rule '{rule.get('name')}' ({file_path})"
        )
    if problems:
        raise ValueError(f"Rule-level validation failed: {problems}")


def validate_with_dqx(rules, file_path: str):
    status = DQEngine.validate_checks(rules)
    if status.has_errors:
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")


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

    # If you want to cast audit timestamps to actual timestamps in the sink,
    # uncomment the next two lines and make sure your Delta schema is evolved accordingly.
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
    for fname in os.listdir(rules_dir):
        if not fname.endswith((".yaml", ".yml")):
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


def main(
    output_config_path: str = "resources/dqx_output_config.yaml",
    rules_dir: str = "resources/dqx_checks_config/",
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False
):
    spark = SparkSession.builder.getOrCreate()

    print_notebook_env(spark, local_timezone=time_zone)

    output_config = parse_output_config(output_config_path)
    delta_table_name = output_config["dqx_config_table_name"]

    all_rules = []
    for fname in os.listdir(rules_dir):
        if not fname.endswith((".yaml", ".yml")):
            continue
        full_path = os.path.join(rules_dir, fname)
        file_rules = process_yaml_file(full_path, output_config, time_zone=time_zone)
        all_rules.extend(file_rules)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        validate_all_rules(rules_dir, output_config)
        return

    if dry_run:
        print_rules_df(spark, all_rules)
        return

    ensure_delta_table(spark, delta_table_name)
    upsert_rules_into_delta(spark, all_rules, delta_table_name)
    print(f"Finished writing rules to '{delta_table_name}'.")


if __name__ == "__main__":
    # main(dry_run=True)
    # main(validate_only=True)
    main()




So the changes I want to make are this

For the rules_dir, I would like to define thisn in the yaml file we currently call output_config_path


This is the current file:
# resources/dqx_output_config.yaml

dqx_config_table_name: dq_dev.dqx.checks_config

run_config_name:
  default:
    output_config:
      location: dq_dev.dqx.checks_log
      mode: overwrite
      options:
        mergeSchema: true
    quarantine_config:
      location: dq_dev.dqx.checks_quarantine
      mode: append
      options:
        mergeSchema: true
  none:
    output_config:
      location: none
      mode: none
      options: {}
    quarantine_config:
      location: none
      mode: none
      options: {}



I would like to rename the file: dqx_config.yaml


also beacuase we define the new value inside it would look like this:
# resources/dqx_output_config.yaml

dqx_checks_config_table_name: dq_dev.dqx.checks_config

dqx_yaml_checks = "resources/dqx_checks_config/"

run_config_name:
  default:
    output_config:
      location: dq_dev.dqx.checks_log
      mode: overwrite
      options:
        mergeSchema: true
    quarantine_config:
      location: dq_dev.dqx.checks_quarantine
      mode: append
      options:
        mergeSchema: true
  none:
    output_config:
      location: none
      mode: none
      options: {}
    quarantine_config:
      location: none
      mode: none
      options: {}




without removing code or making chnages other than i describe can you show me the updated yaml file and the code to apply the checks?


