# Databricks notebook source
# MAGIC %md
# MAGIC # Run DQX Checks

# COMMAND ----------

# === Cell 1: Install DQX Package (if your cluster image doesn't already include it) ===
%pip install databricks-labs-dqx==0.8.0

# COMMAND ----------

# === Cell 2: Restart Python to pick up libs (Databricks convention) ===
dbutils.library.restartPython()

# COMMAND ----------

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

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

def _get(obj, attr, default=None):
    """Works for both dicts and DQX model objects."""
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)

class DQXCheckRunner:
    def __init__(self, spark: SparkSession, engine: DQEngine, debug: bool = True):
        self.spark = spark
        self.engine = engine
        self.debug = debug

    # --- Printing helpers ---
    def print_info(self, msg: str) -> None:
        print(f"{Color.b}{Color.aqua_blue}{msg}{Color.r}")

    def print_warn(self, msg: str) -> None:
        print(f"{Color.b}{Color.yellow}{msg}{Color.r}")

    def print_error(self, msg: str) -> None:
        print(f"{Color.b}{Color.candy_red}{msg}{Color.r}")

    # --- File/YAML helpers ---
    def read_yaml(self, path: str) -> Dict[str, Any]:
        if path.startswith("dbfs:/"):
            path = path.replace("dbfs:/", "/dbfs/")
        with open(path, "r") as fh:
            return yaml.safe_load(fh) or {}

    # --- Table helpers ---
    def ensure_table_with_schema(self, full_name: str) -> None:
        if not self.spark.catalog.tableExists(full_name):
            cat, sch, _ = full_name.split(".")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
            self.spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA) \
                .write.format("delta").mode("overwrite").saveAsTable(full_name)

    def load_checks_for_run_config(self, checks_table: str, rc_name: str) -> List[dict]:
        return self.engine.load_checks(
            config=TableChecksStorageConfig(location=checks_table, run_config_name=rc_name)
        )

    def group_checks_by_table(self, checks: List[dict]) -> Dict[str, List[dict]]:
        """
        Robustly pull table name from either dict or DQX model:
        prefer 'table_name', else 'table'.
        """
        out: Dict[str, List[dict]] = {}
        for c in checks:
            tbl = _get(c, "table_name") or _get(c, "table")
            if tbl:
                out.setdefault(tbl, []).append(c)
        return out

    def _summarize_hits(self, annotated: DataFrame) -> Tuple[int, int]:
        if "_error" not in annotated.columns and "_warning" not in annotated.columns:
            return (0, 0)
        agg = annotated.select(
            F.size(F.col("_error")).alias("e_sz"),
            F.size(F.col("_warning")).alias("w_sz")
        ).agg(
            F.coalesce(F.sum("e_sz"), F.lit(0)).alias("errors"),
            F.coalesce(F.sum("w_sz"), F.lit(0)).alias("warnings")
        ).collect()[0]
        return int(agg["errors"]), int(agg["warnings"])

    def _with_rule_id(
        self,
        exploded: DataFrame,
        source_table: str,
        run_config_name: str
    ) -> DataFrame:
        """
        Attach a deterministic rule_id so we don't have to join back to the config:
        md5(table_name || run_config_name || name)
        """
        return exploded.withColumn(
            "rule_id",
            F.md5(F.concat_ws("||",
                              F.lit(source_table),
                              F.lit(run_config_name),
                              F.coalesce(F.col("name"), F.lit(""))))
        )

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

        exploded = self._with_rule_id(exploded, source_table, run_config_name)

        # select in schema order
        cols_in_order = [f.name for f in DQX_CHECKS_LOG_SCHEMA.fields]
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
            .select(*cols_in_order)
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
            # Adds _error / _warning arrays per row (LOG-ONLY path)
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
        # enforce schema/order
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

        # Load checks from the Delta table (one row = one rule to run)
        try:
            checks = self.load_checks_for_run_config(checks_table, rc_name)
        except Exception as e:
            self.print_error(f"Failed to load checks for {rc_name}: {e}")
            return 0

        if not checks:
            self.print_warn(f"No checks found for run-config '{rc_name}'.")
            return 0

        status = DQEngine.validate_checks(checks)
        if getattr(status, "has_errors", False):
            self.print_error(f"Invalid checks for {rc_name}: {status}")
            return 0

        by_table = self.group_checks_by_table(checks)
        checks_loaded = len(checks)
        tables_targeted = len(by_table)
        self.print_info(f"Loaded {checks_loaded} checks across {tables_targeted} table(s).")

        if tables_targeted:
            self.print_info("Target tables: " + ", ".join(sorted(by_table.keys())))

        # Ensure sink exists
        self.ensure_table_with_schema(results_table)

        # Per-run-config counters
        rc_total_written = 0
        rc_total_err = 0
        rc_total_warn = 0

        # Execute per table
        for src_table, tbl_checks in by_table.items():
            self.print_info(f"Table: {src_table} | checks to run: {len(tbl_checks)}")
            annotated = self.apply_checks_for_table(src_table, tbl_checks)
            if annotated is None:
                continue

            e_cnt, w_cnt = self._summarize_hits(annotated)
            self.print_info(f"  - Raw per-row arrays: errors={e_cnt}, warnings={w_cnt}")

            errors_df   = self.explode_result_array(annotated, "_error",   "error",   src_table, rc_name, created_by)
            warnings_df = self.explode_result_array(annotated, "_warning", "warning", src_table, rc_name, created_by)
            out_df = errors_df.unionByName(warnings_df, allowMissingColumns=True)

            if out_df.rdd.isEmpty():
                self.print_info("  - No error/warning hits after explode.")
                continue

            sev_counts = {r["severity"]: r["count"] for r in out_df.groupBy("severity").count().collect()}
            rc_total_err  += int(sev_counts.get("error", 0))
            rc_total_warn += int(sev_counts.get("warning", 0))

            if self.debug:
                self.print_info("  - Top rules by hit count:")
                out_df.groupBy("rule_id", "name", "severity").count().orderBy(F.desc("count")).show(10, truncate=False)

            written = self.write_hits(out_df, results_table, write_mode, write_options)
            rc_total_written += written
            self.print_info(f"  - Written rows for {src_table}: {written}")

        # Per-run-config summary
        self.print_info(
            f"SUMMARY [{rc_name}] -> checks={checks_loaded}, tables={tables_targeted}, "
            f"hits_err={rc_total_err}, hits_warn={rc_total_warn}, written_rows={rc_total_written}"
        )

        return rc_total_written


def main(
    dqx_config_yaml: str = "resources/dqx_config.yaml",
    results_table_override: Optional[str] = None,
    created_by: str = "AdminUser",
    debug: bool = True
) -> None:
    spark = SparkSession.builder.getOrCreate()
    engine = DQEngine(WorkspaceClient())
    runner = DQXCheckRunner(spark, engine, debug=debug)

    cfg = runner.read_yaml(dqx_config_yaml)
    checks_table = cfg["dqx_checks_config_table_name"]
    global_results_table = results_table_override or cfg["dqx_checks_log_table_name"]

    rc_map: Dict[str, Any] = cfg.get("run_config_name", {}) or {}
    if not rc_map:
        raise ValueError("No run_config_name in config.")

    grand_total = 0
    for rc_name, rc_cfg in rc_map.items():
        if rc_name.lower() == "none":
            # ignore the placeholder profile entirely
            continue

        out_cfg = (rc_cfg or {}).get("output_config", {}) or {}
        out_mode = out_cfg.get("mode", "overwrite")
        out_options = out_cfg.get("options", {}) or {}

        written = runner.apply_for_run_config(
            checks_table=checks_table,
            rc_name=rc_name,
            results_table=global_results_table,
            write_mode=out_mode,
            write_options=out_options,
            created_by=created_by
        )
        grand_total += written

    runner.print_info(f"TOTAL rows written across all run-configs: {grand_total}")


if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing

# COMMAND ----------

import yaml
from collections import defaultdict
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import functions as F

yaml_path = "resources/dqx_checks_config/two_rules_test.yaml"

# 1) Load YAML
with open(yaml_path, "r") as fh:
    rules = yaml.safe_load(fh) or []
if isinstance(rules, dict):
    rules = [rules]

# 2) Group by table
by_table = defaultdict(list)
for r in rules:
    t = r.get("table_name")
    if t:
        by_table[t].append(r)

print(f"Loaded {sum(len(v) for v in by_table.values())} rule(s) across {len(by_table)} table(s): {list(by_table.keys())}")

engine = DQEngine(WorkspaceClient())

# 3) Apply per table
for table, tbl_rules in by_table.items():
    print(f"\n=== Applying {len(tbl_rules)} rule(s) to {table} ===")
    df = spark.table(table)

    annotated = engine.apply_checks_by_metadata(df, tbl_rules)

    # DQX may use _error/_warning or _errors/_warnings depending on version
    err_col = "_errors"  if "_errors"  in annotated.columns else "_error"
    wrn_col = "_warnings" if "_warnings" in annotated.columns else "_warning"

    # Summary
    agg = (
        annotated.select(
            F.size(F.col(err_col)).alias("e_sz"),
            F.size(F.col(wrn_col)).alias("w_sz"),
        )
        .agg(
            F.coalesce(F.sum("e_sz"), F.lit(0)).alias("errors"),
            F.coalesce(F.sum("w_sz"), F.lit(0)).alias("warnings"),
        )
        .collect()[0]
    )
    print(f"Hit summary -> errors={int(agg['errors'])}, warnings={int(agg['warnings'])}")

    # Keep only rows that actually have hits
    hits_only = annotated.filter(
        (F.col(err_col).isNotNull() & (F.size(F.col(err_col)) > 0)) |
        (F.col(wrn_col).isNotNull() & (F.size(F.col(wrn_col)) > 0))
    )

    # Explode per array (no _outer), then union; drop all-null just in case
    errors = (
        hits_only.filter(F.size(F.col(err_col)) > 0)
        .select(F.explode(F.col(err_col)).alias("r"))
        .select("r.*")
    )
    warnings = (
        hits_only.filter(F.size(F.col(wrn_col)) > 0)
        .select(F.explode(F.col(wrn_col)).alias("r"))
        .select("r.*")
    )
    hits = errors.unionByName(warnings, allowMissingColumns=True).na.drop("all")

    print("\nSummary by rule:")
    hits.groupBy("name", "function").count().orderBy(F.desc("count")).show(truncate=False)

    print("\nViolations (one row per hit):")
    try:
        display(hits.select("name", "message", "columns", "filter", "function", "run_time", "user_metadata"))
    except NameError:
        hits.select("name", "message", "columns", "filter", "function", "run_time", "user_metadata").show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Class Defined DQX
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs

# Target table → DataFrame
df = spark.table("dq_prd.monitoring.job_run_audit")

# Define checks as DQX classes
checks = [
    DQRowRule(
        name="run_id_is_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="run_id",
    ),
    DQRowRule(
        name="failed_state_is_terminated",
        criticality="error",
        filter="result_state = 'FAILED'",
        check_func=check_funcs.sql_expression,
        check_func_kwargs={
            "expression": "state = 'TERMINATED'",  # assert condition; fails when not met
            "msg": "FAILED run not in TERMINATED state"
        },
    ),
]

# Apply checks → adds `_warning` / `_error` columns at the end
dq = DQEngine(WorkspaceClient())
df_checked = dq.apply_checks(df, checks)

# Show the result; rows with issues will have non-empty arrays in `_error` / `_warning`
display(df_checked)
# If you only want affected rows during testing:
# display(df_checked.where((F.size("_error") > 0) | (F.size("_warning") > 0)))

# COMMAND ----------

# DBTITLE 1,Yaml Defined Rules
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import FileChecksStorageConfig
from pyspark.sql import functions as F

# 1) Target table → DF
df = spark.table("dq_prd.monitoring.job_run_audit")

# 2) Load checks from YAML (adjust path if your cwd differs)
dq = DQEngine(WorkspaceClient())
checks = dq.load_checks(
    FileChecksStorageConfig(location="resources/dqx_checks_config/two_rules_test.yaml")
)

# 3) Apply checks → appends `_warning` / `_error` array<struct> columns
df_checked = dq.apply_checks_by_metadata(df, checks)

# Show only affected rows (keep full table shape, just filtered)
#display(df_checked.where((F.size("_error") > 0) | (F.size("_warning") > 0)))

# If you want everything, not filtered:
display(df_checked)

# (Optional) Need a flat “result dataframe” of issues matching your schema?
# errs = (df_checked
#   .select(F.explode(F.array_union("_error","_warning")).alias("r"))
#   .select("r.name","r.message","r.columns","r.filter","r.function","r.run_time","r.user_metadata"))
# display(errs)

# COMMAND ----------

# DBTITLE 1,Filter Rows with Existing Errors
# keep rows where _errors exists and has items
df_filtered = df_checked.where(F.col("_errors").isNotNull() & (F.size("_errors") > 0))
display(df_filtered)

# COMMAND ----------

# DBTITLE 1,Generate Stable Error Fingerprints
from pyspark.sql import functions as F

# pick singular vs plural, depending on your DQX version
errors_col = "_errors" if "_errors" in df_filtered.columns else "_error"

# array<struct<column:string,value:string>> for all non-system cols
base_cols = sorted([c for c in df_filtered.columns if not c.startswith("_")])
row_kv = F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).cast("string").alias("value")) for c in base_cols])

# stable fingerprints (JSON of ordered arrays → sha2)
row_kv_fp = F.sha2(F.to_json(row_kv), 256)

# normalize errors for stable hashing (exclude volatile fields like run_time/user_metadata)
err_norm = F.transform(
    F.col(errors_col),
    lambda r: F.struct(
        r["name"].alias("name"),
        r["message"].alias("message"),
        F.array_sort(r["columns"]).alias("columns"),
        r["filter"].alias("filter"),
        r["function"].alias("function"),
    ),
)
errors_fp = F.sha2(F.to_json(F.array_sort(err_norm)), 256)

df_out = df_filtered.select(
    F.col(errors_col).alias("_errors"),
    row_kv.alias("row_kv"),
    row_kv_fp.alias("row_kv_fp"),
    errors_fp.alias("errors_fp"),
)

display(df_out)
