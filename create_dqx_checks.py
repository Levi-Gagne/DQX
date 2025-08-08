import os
import re
import json
from typing import List, Optional, Dict, Any, Literal

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import (
    FileChecksStorageConfig,              # kept for completeness (not used when writing YAML manually)
    WorkspaceFileChecksStorageConfig,     # kept for completeness (not used when writing YAML manually)
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,        # kept for completeness (not used when writing YAML manually)
)
from pyspark.sql import SparkSession


# ===========
# Utilities
# ===========
def glob_to_regex(glob_pattern: str) -> str:
    if not glob_pattern or not glob_pattern.startswith('.'):
        raise ValueError("Exclude pattern must start with a dot, e.g. '.tmp_*'")
    glob = glob_pattern[1:]
    regex = re.escape(glob).replace(r'\*', '.*')
    return '^' + regex + '$'


class RuleGenerator:
    def __init__(
        self,
        mode: str,                       # "pipeline" | "catalog" | "schema" | "table"
        name_param: str,                 # pipeline CSV | catalog | catalog.schema | table FQN CSV
        output_format: str,              # "yaml" | "table"
        output_location: str,            # yaml: folder or file; table: catalog.schema.table
        profile_options: Dict[str, Any],
        exclude_pattern: Optional[str] = None,     # e.g. ".tmp_*"
        created_by: Optional[str] = "LMG",
        columns: Optional[List[str]] = None,       # None => whole table (only if mode=="table")
        run_config_name: str = "default",          # DQX run group tag
        criticality: str = "warn",                 # "warn" | "error"
        yaml_key_order: Literal["engine", "custom"] = "custom",  # keep param; we always format ourselves
        include_table_name: bool = True,           # include table_name in each rule dict
    ):
        self.mode = mode.lower().strip()
        self.name_param = name_param
        self.output_format = output_format.lower().strip()
        self.output_location = output_location
        self.profile_options = (profile_options or {}).copy()
        self.exclude_pattern = exclude_pattern
        self.created_by = created_by
        self.columns = columns
        self.run_config_name = run_config_name
        self.criticality = criticality
        self.yaml_key_order = yaml_key_order
        self.include_table_name = include_table_name

        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("No active Spark session found. Run this in a Databricks notebook.")

        if self.output_format not in {"yaml", "table"}:
            raise ValueError("output_format must be 'yaml' or 'table'.")
        if self.output_format == "yaml" and not self.output_location:
            raise ValueError("When output_format='yaml', provide output_location (folder or file).")

    # ---------- profiler call kwargs ----------
    def _profile_call_kwargs(self) -> Dict[str, Any]:
        """
        DQProfiler.profile/profile_table accept:
          - cols: Optional[List[str]]
          - options: Dict[str, Any] (we pass through verbatim)
        """
        kwargs: Dict[str, Any] = {}
        if self.columns is not None:
            kwargs["cols"] = self.columns
        kwargs["options"] = self.profile_options
        return kwargs

    # ---------- discovery ----------
    def _exclude_tables_by_pattern(self, fq_tables: List[str]) -> List[str]:
        if not self.exclude_pattern:
            return fq_tables
        regex = glob_to_regex(self.exclude_pattern)
        pattern = re.compile(regex)
        filtered = []
        for fq in fq_tables:
            tbl = fq.split('.')[-1]
            if not pattern.match(tbl):
                filtered.append(fq)
        print(f"[INFO] Excluded {len(fq_tables) - len(filtered)} tables by pattern '{self.exclude_pattern}'")
        return filtered

    def _discover_tables(self) -> List[str]:
        print("\n===== PARAMETERS PASSED THIS RUN =====")
        print(f"mode:               {self.mode}")
        print(f"name_param:         {self.name_param}")
        print(f"output_format:      {self.output_format}")
        print(f"output_location:    {self.output_location}")
        print(f"exclude_pattern:    {self.exclude_pattern}")
        print(f"created_by:         {self.created_by}")
        print(f"columns:            {self.columns}")
        print(f"run_config_name:    {self.run_config_name}")
        print(f"criticality:        {self.criticality}")
        print(f"yaml_key_order:     {self.yaml_key_order}")
        print(f"include_table_name: {self.include_table_name}")
        print("PROFILE_OPTIONS (effective):")
        for k, v in self.profile_options.items():
            print(f"  {k}: {v}")
        print("======================================\n")

        allowed_modes = {"pipeline", "catalog", "schema", "table"}
        if self.mode not in allowed_modes:
            raise ValueError(f"Invalid mode '{self.mode}'. Must be one of: {sorted(allowed_modes)}.")
        if self.columns is not None and self.mode != "table":
            raise ValueError("The 'columns' parameter can only be used in mode='table'.")

        discovered: List[str] = []
        if self.mode == "pipeline":
            print("Searching for pipeline output tables...")
            ws = WorkspaceClient()
            pipelines = [p.strip() for p in self.name_param.split(",") if p.strip()]
            print(f"Pipelines passed: {pipelines}")
            for pipeline_name in pipelines:
                print(f"Finding output tables for pipeline: {pipeline_name}")
                pls = list(ws.pipelines.list_pipelines())
                pl = next((p for p in pls if p.name == pipeline_name), None)
                if not pl:
                    raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")
                latest_update = pl.latest_updates[0].update_id
                events = ws.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=250)
                pipeline_tables = [
                    getattr(ev.origin, "flow_name", None)
                    for ev in events
                    if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
                ]
                discovered += [x for x in pipeline_tables if x]

        elif self.mode == "catalog":
            print("Searching for tables in catalog...")
            catalog = self.name_param.strip()
            schemas = [row.namespace for row in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
            for s in schemas:
                tbls = self.spark.sql(f"SHOW TABLES IN {catalog}.{s}").collect()
                discovered += [f"{catalog}.{s}.{row.tableName}" for row in tbls]

        elif self.mode == "schema":
            print("Searching for tables in schema...")
            if self.name_param.count(".") != 1:
                raise ValueError("For 'schema' mode, name_param must be catalog.schema")
            catalog, schema = self.name_param.strip().split(".")
            tbls = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            discovered = [f"{catalog}.{schema}.{row.tableName}" for row in tbls]

        else:  # table
            print("Profiling one or more specific tables...")
            tables = [t.strip() for t in self.name_param.split(",") if t.strip()]
            for t in tables:
                if t.count(".") != 2:
                    raise ValueError(f"Table name '{t}' must be fully qualified (catalog.schema.table)")
            discovered = tables

        print("\nRunning exclude pattern filtering (if any)...")
        discovered = self._exclude_tables_by_pattern(discovered)
        print("\nFinal table list to generate DQX rules for:")
        print(discovered)
        print("==========================================\n")
        return sorted(set(discovered))

    # ---------- storage config helpers ----------
    @staticmethod
    def _infer_file_storage_config(file_path: str):
        if file_path.startswith("/Volumes/"):
            return VolumeFileChecksStorageConfig(location=file_path)
        if file_path.startswith("/"):
            return WorkspaceFileChecksStorageConfig(location=file_path)
        return FileChecksStorageConfig(location=file_path)

    @staticmethod
    def _table_storage_config(table_fqn: str, run_config_name: Optional[str] = None, mode: str = "append"):
        return TableChecksStorageConfig(location=table_fqn, run_config_name=run_config_name, mode=mode)

    @staticmethod
    def _workspace_files_upload(path: str, payload: bytes) -> None:
        wc = WorkspaceClient()
        try:
            wc.files.upload(file_path=path, contents=payload, overwrite=True)  # newer SDK
        except TypeError:
            wc.files.upload(path=path, contents=payload, overwrite=True)       # older SDK

    @staticmethod
    def _ensure_parent(path: str) -> None:
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

    @staticmethod
    def _ensure_dbfs_parent(dbutils, path: str) -> None:
        parent = path.rsplit("/", 1)[0] if "/" in path else path
        if parent:
            dbutils.fs.mkdirs(parent)

    # ---------- DQX check shaping ----------
    def _dq_constraint_to_check(
        self,
        rule_name: str,
        constraint_sql: str,
        table_name: str,
        criticality: str,
        run_config_name: str
    ) -> Dict[str, Any]:
        d = {
            "name": rule_name,
            "criticality": criticality,
            "run_config_name": run_config_name,
            "check": {
                "function": "sql_expression",
                "arguments": {
                    "expression": constraint_sql,
                    "name": rule_name,
                }
            },
        }
        if self.include_table_name:
            d = {"table_name": table_name, **d}
        return d

    # ---------- YAML writers (always overwrite + blank line between rules) ----------
    def _format_yaml_with_blank_lines(self, checks: List[Dict[str, Any]]) -> str:
        """
        Emit YAML with a blank line between top-level list items.
        """
        dumped = yaml.safe_dump(checks, sort_keys=False, default_flow_style=False).rstrip()
        return re.sub(r"\n- ", "\n\n- ", dumped) + "\n"

    def _write_yaml_ordered(self, checks: List[Dict[str, Any]], path: str) -> None:
        """
        Dump YAML preserving key order, add blank line between rules,
        and overwrite destination (DBFS/Volumes/Workspace/local).
        """
        yaml_str = self._format_yaml_with_blank_lines(checks)

        # DBFS / Volumes
        if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("/Volumes/"):
            try:
                from databricks.sdk.runtime import dbutils
            except Exception:
                raise RuntimeError("dbutils is required to write to DBFS/Volumes.")
            target = path if path.startswith("dbfs:/") else (f"dbfs:{path}" if not path.startswith("dbfs:") else path)
            self._ensure_dbfs_parent(dbutils, target.rsplit("/", 1)[0])
            dbutils.fs.put(target, yaml_str, True)  # overwrite=True
            print(f"[RUN] Wrote YAML (overwritten) to {path}")
            return

        # Workspace files
        if path.startswith("/"):
            self._workspace_files_upload(path, yaml_str.encode("utf-8"))  # overwrite=True
            print(f"[RUN] Wrote YAML (overwritten) to workspace file: {path}")
            return

        # Local (driver) filesystem
        full_path = os.path.abspath(path)
        self._ensure_parent(full_path)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(yaml_str)  # overwrite
        print(f"[RUN] Wrote YAML (overwritten) to local path: {full_path}")

    # ---------- main ----------
    def run(self):
        try:
            tables = self._discover_tables()
            print("[RUN] Beginning DQX rule generation on these tables:")
            for t in tables:
                print(f"  {t}")
            print("==========================================\n")

            call_kwargs = self._profile_call_kwargs()
            print("[RUN] Profiler call kwargs:")
            print(f"  cols:    {call_kwargs.get('cols')}")
            print(f"  options: {json.dumps(call_kwargs.get('options', {}), indent=2)}")

            dq_engine = DQEngine(WorkspaceClient())
            total_checks = 0

            for fq_table in tables:
                if fq_table.count(".") != 2:
                    print(f"[WARN] Skipping invalid table name: {fq_table}")
                    continue
                _, _, tab = fq_table.split(".")

                # Verify readability
                try:
                    print(f"[RUN] Checking table readability: {fq_table}")
                    self.spark.table(fq_table).limit(1).collect()
                except Exception as e:
                    print(f"[WARN] Table {fq_table} not readable in Spark: {e}")
                    continue

                profiler = DQProfiler(WorkspaceClient())
                generator = DQDltGenerator(WorkspaceClient())
                df = self.spark.table(fq_table)

                try:
                    print(f"[RUN] Profiling and generating rules for: {fq_table}")
                    summary_stats, profiles = profiler.profile(df, **call_kwargs)

                    # If you prefer the table API instead:
                    # summary_stats, profiles = profiler.profile_table(table=fq_table, **call_kwargs)

                    rules_dict = generator.generate_dlt_rules(profiles, language="Python_Dict")
                except Exception as e:
                    print(f"[WARN] Profiling failed for {fq_table}: {e}")
                    continue

                checks: List[Dict[str, Any]] = []
                for rule_name, constraint in (rules_dict or {}).items():
                    checks.append(
                        self._dq_constraint_to_check(
                            rule_name=rule_name,
                            constraint_sql=constraint,
                            table_name=fq_table,
                            criticality=self.criticality,
                            run_config_name=self.run_config_name,
                        )
                    )

                if not checks:
                    print(f"[INFO] No checks generated for {fq_table}.")
                    continue

                # Destination selection
                if self.output_format == "yaml":
                    # Directory -> {table}.yaml ; or exact file path
                    if self.output_location.endswith((".yaml", ".yml")):
                        path = self.output_location
                    else:
                        path = f"{self.output_location.rstrip('/')}/{tab}.yaml"

                    print(f"[RUN] Saving {len(checks)} checks with spacing to: {path} (overwrites file)")
                    self._write_yaml_ordered(checks, path)
                    total_checks += len(checks)

                else:  # table sink (append)
                    cfg = self._table_storage_config(
                        table_fqn=self.output_location,
                        run_config_name=self.run_config_name,
                        mode="append"
                    )
                    print(f"[RUN] Appending {len(checks)} checks to table: {self.output_location} (run_config_name={self.run_config_name})")
                    dq_engine.save_checks(checks, config=cfg)
                    total_checks += len(checks)

            print(f"[RUN] {'Successfully saved' if total_checks else 'No'} checks. Count: {total_checks}")
        except Exception as e:
            print(f"[ERROR] Rule generation failed: {e}")


# -------------------- Usage (define defaults HERE, near the entrypoint) --------------------
if __name__ == "__main__":
    # ======================================================
    # Profile Options — CLA Defaults (explicit)
    # Teams can edit these in-place. Passed verbatim to DQProfiler via options=...
    # Notes:
    # - DQX historically sampled (~0.3) and limited rows (~1000). We pin them explicitly here.
    # - Keep the commented keys below to reflect doc surface; uncomment to try them as DQX evolves.
    # ======================================================
    profile_options: Dict[str, Any] = {
        # Sampling
        "sample_fraction": 0.3,     # fraction of rows to sample
        "sample_seed": None,        # set for reproducibility
        "limit": 1000,              # cap records after sampling

        # Outliers (min/max rules)
        "remove_outliers": False,   # when True, min/max ignore outliers
        "outlier_columns": [],      # limit outlier handling to specific columns
        "num_sigmas": 3,            # Nσ for outlier detection

        # Null / empty thresholds → rule suggestions
        "max_null_ratio": 0.05,     # suggest is_not_null if <5% nulls
        "trim_strings": True,       # trim before empty checks
        "max_empty_ratio": 0.02,    # suggest not_empty if <2% empties

        # Distinct → is_in
        "distinct_ratio": 0.01,     # if <1% distinct, suggest is_in
        "max_in_count": 20,         # max items in is_in list

        # Readability
        "round": True,              # round numeric thresholds for cleaner rules

        # ---- Optional / documented historically; keep for reference ----
        # "include_histograms": False,
        # "min_length": None,
        # "max_length": None,
        # "min_value": None,
        # "max_value": None,
        # "profile_types": None,
    }

    RuleGenerator(
        mode="table",                                   # "pipeline" | "catalog" | "schema" | "table"
        name_param="dq_prd.monitoring.job_run_audit",   # depends on mode
        output_format="yaml",                           # "yaml" | "table"
        output_location="dqx_checks",                   # yaml dir OR a full file path; file will be OVERWRITTEN
        profile_options=profile_options,
        columns=None,                                   # None => whole table (only valid when mode=="table")
        exclude_pattern=None,                           # e.g. ".tmp_*"
        created_by="LMG",
        run_config_name="default",
        criticality="warn",
        yaml_key_order="custom",                        # ensure blank line spacing between rules
        include_table_name=True,
    ).run()