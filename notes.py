So this is the code that we are working with to run the checks we just loaded:


# apply_dqx_checks.py

from __future__ import annotations
from typing import Dict, Any, List, Optional
import os
import yaml

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from utils.color import Color

# =========================
# Schema for DQX checks log
# =========================
DQX_CHECKS_LOG_SCHEMA = T.StructType([
    T.StructField("result_id",       T.StringType(),  False),
    T.StructField("rule_id",         T.StringType(),  False),
    T.StructField("source_table",    T.StringType(),  False),
    T.StructField("run_config_name", T.StringType(),  False),
    T.StructField("severity",        T.StringType(),  False),
    T.StructField("name",            T.StringType(),  True),
    T.StructField("message",         T.StringType(),  True),
    T.StructField("columns",         T.ArrayType(T.StringType()), True),
    T.StructField("filter",          T.StringType(),  True),
    T.StructField("function",        T.StringType(),  True),
    T.StructField("run_time",        T.TimestampType(), True),
    T.StructField("user_metadata",   T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("created_by",      T.StringType(),  False),
    T.StructField("created_at",      T.TimestampType(), False),
    T.StructField("updated_by",      T.StringType(),  True),
    T.StructField("updated_at",      T.TimestampType(), True)
])


class DQXCheckRunner:
    def __init__(self, spark: SparkSession, engine: DQEngine):
        self.spark = spark
        self.engine = engine

    # --- Printing helpers ---
    def print_info(self, msg: str) -> None:
        print(f"{Color.b}{Color.aqua_blue}{msg}{Color.r}")

    def print_warn(self, msg: str) -> None:
        print(f"{Color.b}{Color.yellow}{msg}{Color.r}")

    def print_error(self, msg: str) -> None:
        print(f"{Color.b}{Color.candy_red}{msg}{Color.r}")

    # --- File/YAML helpers ---
    def read_yaml(self, path: str) -> Dict[str, Any]:
        # Adapt for DBFS or workspace paths
        if path.startswith("dbfs:/"):
            path = path.replace("dbfs:/", "/dbfs/")
        elif path.startswith("/Workspace/"):
            # Workspace-relative; adjust if needed to actual FS mount
            pass
        with open(path, "r") as fh:
            return yaml.safe_load(fh) or {}

    # --- Table helpers ---
    def ensure_table_with_schema(self, full_name: str) -> None:
        if not self.spark.catalog.tableExists(full_name):
            cat, sch, _ = full_name.split(".")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
            self.spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA) \
                .write.format("delta").mode("overwrite").saveAsTable(full_name)

    def build_rule_id_lookup(self, checks_table: str) -> DataFrame:
        return (
            self.spark.read.table(checks_table)
            .select(
                F.col("table_name").alias("hm_table_name"),
                F.col("run_config_name").alias("hm_run_config_name"),
                F.col("name").alias("hm_name"),
                F.col("hash_id").alias("rule_id"),
            ).dropDuplicates()
        )

    def load_checks_for_run_config(self, checks_table: str, rc_name: str) -> List[dict]:
        return self.engine.load_checks(
            config=TableChecksStorageConfig(location=checks_table, run_config_name=rc_name)
        )

    def group_checks_by_table(self, checks: List[dict]) -> Dict[str, List[dict]]:
        out: Dict[str, List[dict]] = {}
        for c in checks:
            t = c.get("table_name")
            if t:
                out.setdefault(t, []).append(c)
        return out

    def explode_result_array(
        self,
        df: DataFrame,
        array_col: str,
        severity_literal: str,
        source_table: str,
        run_config_name: str,
        created_by: str
    ) -> DataFrame:
        if array_col not in df.columns:
            return self.spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA)
        exploded = df.select(F.explode_outer(F.col(array_col)).alias("r")).select("r.*")
        if exploded.rdd.isEmpty():
            return self.spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA)
        cols_no_rule_id = [f.name for f in DQX_CHECKS_LOG_SCHEMA.fields if f.name != "rule_id"]
        return (
            exploded
            .withColumn("result_id",       F.expr("uuid()"))
            .withColumn("source_table",    F.lit(source_table))
            .withColumn("run_config_name", F.lit(run_config_name))
            .withColumn("severity",        F.lit(severity_literal))
            .withColumn("created_by",      F.lit(created_by))
            .withColumn("created_at",      F.current_timestamp())
            .withColumn("updated_by",      F.lit(None).cast(T.StringType()))
            .withColumn("updated_at",      F.lit(None).cast(T.TimestampType()))
            .select(cols_no_rule_id)
        )

    def apply_checks_for_table(
        self, src_table: str, tbl_checks: List[dict]
    ) -> Optional[DataFrame]:
        try:
            df_src = self.spark.read.table(src_table)
        except Exception as e:
            self.print_error(f"Cannot read {src_table}: {e}")
            return None
        try:
            return self.engine.apply_checks_by_metadata(df_src, tbl_checks)
        except Exception as e:
            self.print_error(f"apply_checks_by_metadata failed for {src_table}: {e}")
            return None

    def write_hits(
        self, out_df: DataFrame, results_table: str, mode: str, options: Dict[str, str]
    ) -> int:
        if out_df.rdd.isEmpty():
            self.print_info("No rows to write.")
            return 0
        out_df = out_df.select([f.name for f in DQX_CHECKS_LOG_SCHEMA.fields])
        written = out_df.count()
        out_df.write.format("delta").mode(mode).options(**options).saveAsTable(results_table)
        return written

    def apply_for_run_config(
        self,
        checks_table: str,
        rc_name: str,
        results_table: str,
        write_mode: str,
        write_options: Dict[str, str],
        created_by: str
    ) -> int:
        self.print_info(f"RUN-CONFIG: {rc_name}")
        self.ensure_table_with_schema(results_table)

        try:
            checks = self.load_checks_for_run_config(checks_table, rc_name)
        except Exception as e:
            self.print_error(f"Failed to load checks for {rc_name}: {e}")
            return 0

        if not checks:
            self.print_warn(f"No checks found for {rc_name}")
            return 0

        # Fail-fast validation (even if done upstream)
        status = DQEngine.validate_checks(checks)
        if getattr(status, "has_errors", False):
            self.print_error(f"Invalid checks for {rc_name}: {status}")
            return 0

        hash_map_df = F.broadcast(self.build_rule_id_lookup(checks_table))
        by_table = self.group_checks_by_table(checks)
        total_written = 0

        for src_table, tbl_checks in by_table.items():
            self.print_info(f"Table: {src_table} | checks: {len(tbl_checks)}")
            annotated = self.apply_checks_for_table(src_table, tbl_checks)
            if annotated is None:
                continue

            errors_df   = self.explode_result_array(annotated, "_error",   "error",   src_table, rc_name, created_by)
            warnings_df = self.explode_result_array(annotated, "_warning", "warning", src_table, rc_name, created_by)
            out_df = errors_df.unionByName(warnings_df, allowMissingColumns=True)
            if out_df.rdd.isEmpty():
                self.print_info("No error/warning hits.")
                continue

            out_df = out_df.join(
                hash_map_df,
                (out_df.source_table    == hash_map_df.hm_table_name) &
                (out_df.run_config_name == hash_map_df.hm_run_config_name) &
                (out_df.name            == hash_map_df.hm_name),
                "left"
            ).drop("hm_table_name", "hm_run_config_name", "hm_name")

            missing = out_df.filter(F.col("rule_id").isNull()).count()
            if missing > 0:
                self.print_warn(f"{missing} hit(s) missing rule_id — dropped.")
                out_df = out_df.filter(F.col("rule_id").isNotNull())
            if out_df.rdd.isEmpty():
                continue

            written = self.write_hits(out_df, results_table, write_mode, write_options)
            total_written += written
            self.print_info(f"Wrote {written} rows to {results_table} for {src_table}")

        return total_written


def main(
    dqx_config_yaml: str = "resources/dqx_config.yaml",
    results_table_override: Optional[str] = None,
    created_by: str = "AdminUser"
) -> None:
    spark = SparkSession.builder.getOrCreate()
    engine = DQEngine(WorkspaceClient())
    runner = DQXCheckRunner(spark, engine)

    cfg = runner.read_yaml(dqx_config_yaml)
    checks_table = cfg["dqx_checks_config_table_name"]
    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}
    if not rc_map:
        raise ValueError("No run_config_name in config.")

    grand_total = 0
    for rc_name, rc_cfg in rc_map.items():
        out_cfg = (rc_cfg or {}).get("output_config", {})
        out_loc = results_table_override or out_cfg.get("location")
        out_mode = out_cfg.get("mode", "overwrite")  # default overwrite
        out_options = out_cfg.get("options", {}) or {}
        if not out_loc or out_loc.lower() == "none":
            runner.print_warn(f"{rc_name} has no output location — skipping.")
            continue
        grand_total += runner.apply_for_run_config(
            checks_table=checks_table,
            rc_name=rc_name,
            results_table=out_loc,
            write_mode=out_mode,
            write_options=out_options,
            created_by=created_by
        )

    runner.print_info(f"TOTAL rows written: {grand_total}")


if __name__ == "__main__":
    main()




Im getting this error:
RUN-CONFIG: default
none has no output location — skipping.
TOTAL rows written: 0


So this is currently how the yaml looks:
# resources/dqx_config.yaml

# Where the YAML rule files live (directory or volume path)
dqx_yaml_checks: resources/dqx_checks_config/

# Where to store flattened DQX rules (Delta table)
dqx_checks_config_table_name: dq_dev.dqx.checks_config

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


I would like to have the ouput location in which we write to defined here as well so it might look like this:
# resources/dqx_config.yaml

# Where the YAML rule files live (directory or volume path)
dqx_yaml_checks: resources/dqx_checks_config/

# Where to store flattened DQX rules (Delta table)
dqx_checks_config_table_name: dq_dev.dqx.checks_config

dqx_checks_log_table_name: dq_dev.dqx.checks_log

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


do you think you can make that change for me please?


Also a note, that the current version of the code that loads teh rules is here, Im not sure if we are using teh correct schema for the other tables primary column as i made updates to this and not the other


So assume the other table works, and make the code above, work with waht i have pasted below, as teh code below we will assume isnt changing:
import os
import json
import hashlib
import yaml
from typing import Dict, Any, Optional

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
    # Expect the new keys
    required = ["dqx_checks_config_table_name", "dqx_yaml_checks", "run_config_name"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")
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

    # Cast audit timestamps to actual TIMESTAMP in the sink
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
    output_config_path: str = "resources/dqx_config.yaml",
    rules_dir: Optional[str] = None,
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False
):
    spark = SparkSession.builder.getOrCreate()

    print_notebook_env(spark, local_timezone=time_zone)

    output_config = parse_output_config(output_config_path)

    # pick up rules_dir from config if not provided explicitly
    rules_dir = rules_dir or output_config["dqx_yaml_checks"]

    delta_table_name = output_config["dqx_checks_config_table_name"]

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
