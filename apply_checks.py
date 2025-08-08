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
    output_config_yaml: str = "dqx_output_config.yaml",
    results_table_override: Optional[str] = None,
    created_by: str = "AdminUser"
) -> None:
    spark = SparkSession.builder.getOrCreate()
    engine = DQEngine(WorkspaceClient())
    runner = DQXCheckRunner(spark, engine)

    cfg = runner.read_yaml(output_config_yaml)
    checks_table = cfg["dqx_config_table_name"]
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