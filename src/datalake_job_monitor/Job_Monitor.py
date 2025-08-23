# Databricks notebook source
# MAGIC %md
# MAGIC # Datalake Job Monitor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC  
# MAGIC This Job Monitor provides continuous visibility into Databricks job executions, capturing key metrics—such as run success/failure frequency and duration—over time. By centralizing run data in a Delta Lake table, teams can proactively identify issues and bottlenecks. While resource utilization and optimized resource allocation are not implemented today, this framework serves as a base table for future enhancements in those areas. "You can’t manage what you don’t measure."
# MAGIC
# MAGIC ---
# MAGIC ### Features  
# MAGIC Implements a production‑grade Databricks **Job Monitor** notebook that:
# MAGIC - **Fetches** job run records from the Databricks REST API  
# MAGIC   (endpoints: `job_runs_list`, `job_run_details`, `job_details`)
# MAGIC - **Flattens** each run’s JSON into a uniform, columnar schema matching our Delta Lake table  
# MAGIC - **Supports** both full loads (using `historical_load_date`) and incremental loads  
# MAGIC   (using max(`start_time_raw`) from existing data)
# MAGIC - **Updates** non‑terminal runs (`RUNNING`, `PENDING`, `QUEUED`) via SQL `UPDATE`  
# MAGIC   to refresh `duration`, `state`, `updated_at`, and `updated_by` once they complete
# MAGIC - **Merges** new, updated, and existing terminal runs into the target Delta table  
# MAGIC   with deduplication on the unique key `run_id`
# MAGIC - **Validates** the final row count after merge
# MAGIC ---
# MAGIC ### General Rules
# MAGIC - **Table**: `yaml_config.full_table_name` (e.g., `dq_prd.monitoring.job_run_audit`)
# MAGIC - **Partition**: by `start_date` (derived from `start_time` timestamp)
# MAGIC - **Deduplication key**: `run_id` (unique per job run)
# MAGIC - **Job identifiers**:  
# MAGIC   - `job_id` (int) and `job_name` (string) identify the job  
# MAGIC   - `run_id` identifies each execution  
# MAGIC - **Timestamps**:  
# MAGIC   - `start_time_raw`: raw epoch ms of run start  
# MAGIC   - `start_time`: converted TIMESTAMP in configured `time_zone`  
# MAGIC   - `start_date`: DATE portion for partitioning  
# MAGIC - **Plumbing columns**:  
# MAGIC   - `created_at`: UTC timestamp when record is first written  
# MAGIC   - `created_by`: default from `schema.plumbing.created_by`  
# MAGIC   - `updated_at` / `updated_by`: NULL on first write; set when run is updated  
# MAGIC - **Table creation & load rules**:  
# MAGIC   - If the Delta table does not exist, `DeltaTableUpdater.create_table_if_not_exists()`  
# MAGIC     builds it from the StructType (schema.business + schema.plumbing) and loads fully  
# MAGIC     from `historical_load_date`.  
# MAGIC   - If the table exists but is empty, a full load uses `historical_load_date`.  
# MAGIC   - If the table contains data, an incremental load uses max(`start_time_raw`) as cutoff  
# MAGIC     and fetches all runs since that millisecond timestamp.
# MAGIC ---
# MAGIC ### Main Workflow (`update_job_runs`)
# MAGIC 1. **Read Existing Data**  
# MAGIC    ```python
# MAGIC    existing_df = table_updater.read_table_as_df().select(*schema.fieldNames())
# MAGIC    existing_df.cache()
# MAGIC    ```
# MAGIC 2. **Determine Cutoff**  
# MAGIC    ```python
# MAGIC    full_load, cutoff_ms = _determine_cutoff(existing_df)
# MAGIC    ```
# MAGIC    - **Full load** if `existing_df.count()==0`:  
# MAGIC      `cutoff_ms = historical_load_date → epoch ms`  
# MAGIC    - **Incremental** otherwise:  
# MAGIC      `cutoff_ms = existing_df.agg(max("start_time_raw"))`
# MAGIC 3. **Fetch Runs from API**  
# MAGIC    ```python
# MAGIC    api_runs = api_client.get_all_runs_since(cutoff_ms)
# MAGIC    ```
# MAGIC    - Handles pagination and filters out runs older than `cutoff_ms` unless still running.
# MAGIC 4. **Process New Runs**  
# MAGIC    ```python
# MAGIC    new_runs_df = process_new_api_runs(api_runs)
# MAGIC    ```
# MAGIC    - Applies `JobRunProcessor.build_row_for_run()` to each JSON dict  
# MAGIC    - Returns a Spark DataFrame typed to the target schema
# MAGIC 5. **Update Pending Runs**  
# MAGIC    ```python
# MAGIC    pending_df = get_pending_runs_df(existing_df)
# MAGIC    update_pending_runs_sql(pending_df)
# MAGIC    ```
# MAGIC    - For each pending `run_id`:  
# MAGIC      - Fetch fresh details via `get_run_details(run_id)`  
# MAGIC      - Flatten to `updated_row`  
# MAGIC      - Compute new `duration` or `state`  
# MAGIC      - Execute:
# MAGIC        ```sql
# MAGIC        UPDATE <table>
# MAGIC        SET duration=…, state=…, updated_at=timestamp'…', updated_by='…'
# MAGIC        WHERE run_id='…'
# MAGIC        ```
# MAGIC 6. **Merge All Changes**  
# MAGIC    ```python
# MAGIC    _merge_updates(existing_df, updated_pending_df, new_runs_df)
# MAGIC    ```
# MAGIC    - **terminal_df** = existing runs where `state` NOT IN (RUNNING, PENDING, QUEUED)  
# MAGIC    - **combined** = terminal_df ∪ updated_pending_df ∪ new_runs_df  
# MAGIC    - Deduplicate by `run_id` with Window ordering on `updated_at`  
# MAGIC    - `DeltaTableUpdater.merge_df(deduped_df, "target.run_id = source.run_id")`
# MAGIC 7. **Validation**  
# MAGIC    ```python
# MAGIC    final_df = table_updater.read_table_as_df().select(*schema.fieldNames())
# MAGIC    final_count = final_df.count()
# MAGIC    ```
# MAGIC    - Logs `final_count` to confirm merge success
# MAGIC
# MAGIC ### Execution Entry Point
# MAGIC ```python
# MAGIC if __name__ == "__main__":
# MAGIC     spark = SparkSession.builder.getOrCreate()
# MAGIC     env, processing_timezone, local_timezone = EnvironmentConfig().environment
# MAGIC
# MAGIC     yaml_config = YamlConfig("yaml/job-monitor.yaml", spark=spark)
# MAGIC     databricks_instance, token = TokenConfig("yaml/token-management.yaml", env, spark).load_secrets()
# MAGIC     ClusterInfo(spark, databricks_instance, token, local_timezone, print_env_var=True)
# MAGIC
# MAGIC     monitor = JobMonitor(
# MAGIC         databricks_instance=databricks_instance,
# MAGIC         token=token,
# MAGIC         table_name=yaml_config.full_table_name,
# MAGIC         yaml_config=yaml_config,
# MAGIC         spark_session=spark,
# MAGIC         api_config=yaml_config.config["app_config"]["api"],
# MAGIC         time_zone=processing_timezone
# MAGIC     )
# MAGIC     monitor.update_job_runs()
# MAGIC ```
# MAGIC ---
# MAGIC ### Custom Modules & Config Files
# MAGIC - **utils.colorConfig**           — ANSI & RGB terminal color codes  
# MAGIC - **utils.messageConfig**         — Cluster & notebook info printing  
# MAGIC - **utils.yamlConfig**            — Loads `yaml/job-monitor.yaml`, builds `full_table_name`  
# MAGIC - **utils.deltaConfig**           — `DeltaTableUpdater`: create/read/merge Delta tables  
# MAGIC - **utils.databricksAPIConfig**   — `DatabricksAPIClient`: REST API wrapper  
# MAGIC - **utils.environmentConfig**     — Reads `ENV`, `TIMEZONE`, `LOCAL_TIMEZONE`  
# MAGIC - **utils.tokenConfig**           — Reads `yaml/token-management.yaml`, retrieves OAuth token & URL  
# MAGIC - **yaml/token-management.yaml**  — Secret definitions for OAuth client credentials  
# MAGIC - **yaml/job-monitor.yaml**       — API endpoints, schema definitions, and app settings  
# MAGIC ---
# MAGIC ### Configuration
# MAGIC This notebook reads two YAML files:
# MAGIC
# MAGIC - **`yaml/job-monitor.yaml`** (via `YamlConfig`):
# MAGIC   - `app_config.api.version`  
# MAGIC   - `app_config.api.endpoints`  
# MAGIC   - `schema.business` & `schema.plumbing`  
# MAGIC   - `app_config.plumbing_columns.dl_partition_column`  
# MAGIC   - `app_config.plumbing_columns.dl_primary_key_column`  
# MAGIC   - `app_config.historical_load_date`  
# MAGIC
# MAGIC - **`yaml/token-management.yaml`** (via `TokenConfig`):
# MAGIC   - `databricks_token_config.{env}.databricks_token.*`  
# MAGIC   - `databricks_token_config.{env}.databricks_instance.*` 
# MAGIC ---
# MAGIC ### Usage
# MAGIC Run all cells in this notebook to execute the full job‑monitor process.  
# MAGIC Adjust `yaml/job-monitor.yaml` and `yaml/token-management.yaml` for your environment as needed.
# MAGIC ---
# MAGIC ### Metadata
# MAGIC - **Author:** Levi Gagne  
# MAGIC - **Created Date:** 2025‑03‑20  
# MAGIC - **Last Modified:** 2025‑04‑18

# COMMAND ----------

# DBTITLE 1,Execution Flow
"""
START: update_job_runs

|
|-- 1. Check if audit table exists:
|     |-- If table does NOT exist:
|     |     |-- Create empty Delta table with target schema and partition.
|     |     |-- Load ALL runs from API since historical_load_date.
|     |     |-- Insert ALL loaded runs into Delta table (append).
|     |     |-- SKIP to step 7 (no existing runs, nothing pending).
|     |
|     |-- If table DOES exist:
|           |-- 2. Read all rows from table into DataFrame and CACHE.
|           |-- 3. COUNT rows in DataFrame.
|                 |-- If count == 0:
|                 |     |-- Go to 1.b: Load ALL from historical_load_date (table structure exists, but is empty).
|                 |-- Else:
|           |-- 4. Find max(start_time_raw) from cached DataFrame (most recent loaded run).
|           |-- 5. Fetch NEW runs from API, **starting at cutoff = max(start_time_raw)**.
|           |-- 6. Flatten/process new runs to match schema.
|           |-- 7. Append new runs into Delta table (DataFrame `.write(mode="append")` or SQL INSERT).
|
|-- 8. Update non-terminal runs (RUNNING, PENDING, QUEUED):
|     |-- From the (cached) DataFrame, FILTER for records where state in ('RUNNING', 'PENDING', 'QUEUED').
|     |-- For each such run_id:
|           |-- Query Databricks API for latest status/details.
|           |-- If new state is TERMINAL:
|           |     |-- UPDATE Delta table row WHERE run_id=...:
|           |           |-- SET: state, duration, updated_at, updated_by
|           |-- Else (still running/pending/queued):
|                 |-- UPDATE Delta table row WHERE run_id=...:
|                       |-- SET: duration, updated_at, updated_by
|
|-- 9. Validation:
|     |-- Read all rows from Delta table into DataFrame (or use previous + appended count if cached).
|     |-- COUNT total rows.
|     |-- CHECK for duplicate run_id:
|           |-- If any duplicates, print offending run_ids and RAISE error.
|           |-- Else, proceed.
|
|-- END: update_job_runs
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target Table Schema  
# MAGIC > `dq_prd.monitoring.job_run_audit`
# MAGIC
# MAGIC | `col_name`             | `data_type` | definition                                                     |
# MAGIC |------------------------|-------------|----------------------------------------------------------------|
# MAGIC | `job_name`             | string      | Name of the Databricks job.                                    |
# MAGIC | `job_id`               | string      | Identifier for the job definition.                             |
# MAGIC | `run_id`               | string      | Unique identifier for each job run (primary key).              |
# MAGIC | `start_time`           | timestamp   | When the run began (partition column).                         |
# MAGIC | `start_date`           | date        | Date portion of `start_time` (for partition pruning).          |
# MAGIC | `state`                | string      | Current lifecycle state (see Reference below).                 |
# MAGIC | `result_state`         | string      | Final outcome (see Reference below).                           |
# MAGIC | `creator`              | string      | User who owns or defined the job.                              |
# MAGIC | `launched_by`          | string      | User who triggered this run.                                   |
# MAGIC | `start_time_raw`       | bigint      | Raw epoch ms when the run started.                             |
# MAGIC | `end_time`             | timestamp   | When the run ended.                                            |
# MAGIC | `duration`             | string      | Wall-clock (`end_time` − `start_time`).                       |
# MAGIC | `queue_duration`       | string      | Time in queue before execution (ms).                           |
# MAGIC | `run_as`               | string      | User context under which the run executed.                     |
# MAGIC | `description`          | string      | Job or run description.                                        |
# MAGIC | `run_page_url`         | string      | URL to view this run in the Databricks UI.                     |
# MAGIC | `tags`                 | string      | Pipe-delimited key/value pairs from the job’s tags.            |
# MAGIC | `email_notification`   | string      | Pipe-delimited email addresses for notifications.              |
# MAGIC | `notification_settings`| string      | Pipe-delimited settings (on start, success, failure).          |
# MAGIC | `error_code`           | string      | Error code if the run failed.                                  |
# MAGIC | `timezone_id`          | string      | IANA timezone ID for job scheduling.                           |
# MAGIC | `schedule_type`        | string      | Scheduling type (cron vs. interval).                           |
# MAGIC | `schedule`             | string      | Cron expression or interval.                                   |
# MAGIC | `pause_status`         | string      | Job paused (`PAUSED`) or active.                              |
# MAGIC | `msteams_notification` | int         | `0`=none; `1`=failure message sent.                            |
# MAGIC | `created_at`           | timestamp   | UTC when record was first inserted.                            |
# MAGIC | `created_by`           | string      | Default creator (from plumbing schema).                        |
# MAGIC | `updated_at`           | timestamp   | UTC when record was last updated.                              |
# MAGIC | `updated_by`           | string      | Who last modified the record.                                  |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Primary Key
# MAGIC
# MAGIC | col_name | data_type | definition                                                   |
# MAGIC |------------|-------------|--------------------------------------------------------------|
# MAGIC | `run_id`   | string      | Primary key, defined by `dl_primary_key_column` in YAML.     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition Column
# MAGIC
# MAGIC | col_name   | data_type | definition                                            |
# MAGIC |--------------|-------------|-------------------------------------------------------|
# MAGIC | `start_date` | date        | Partition defined by `dl_partition_column` in YAML.   |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Scheduling
# MAGIC
# MAGIC - **Databricks Job Name**: `job_run_audit_load`  
# MAGIC - **Schedule**: daily at 00:15 UTC (configured in Jobs UI)  
# MAGIC - **Cluster**: runs on `dataOps` cluster  
# MAGIC - **Downstream Dependency**: `MSTeams_Webhook` notebook runs after successful completion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change Log
# MAGIC
# MAGIC | Date       | Version | Description                                                                          |
# MAGIC |------------|---------|--------------------------------------------------------------------------------------|
# MAGIC | 2025-03-15 | 1.0.0   | Initial notebook creation with core classes and pipeline logic.                     |
# MAGIC | 2025-03-25 | 1.1.0   | Added pending run updates, environment-based time zones, and SQL update logic.      |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference

# COMMAND ----------

# MAGIC %md
# MAGIC #### Terminology
# MAGIC
# MAGIC - **Job run**: an instance of a Databricks job execution, composed of leaf tasks.  
# MAGIC - **state** (`life_cycle_state`): current phase (queued, running, terminated, etc.).  
# MAGIC - **result_state**: final outcome once a run completes.  
# MAGIC - **run_id**: unique primary key for each job run.  
# MAGIC - **start_time_raw**: raw epoch milliseconds when the run started.  
# MAGIC - **dl_partition_column**: configuration key (in `job-monitor.yaml`) defining the partition column.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lifecycle vs. Result States
# MAGIC
# MAGIC `state` tracks where a run is in its lifecycle; `result_state` records the terminal outcome.
# MAGIC
# MAGIC #### `state` (life_cycle_state)
# MAGIC
# MAGIC | state                     | definition                                                            |
# MAGIC |-----------------------------|--------------------------------------------------------------------------|
# MAGIC | `QUEUED` / `PENDING`        | The run is queued and awaiting resources.                                |
# MAGIC | `RUNNING`                   | The run is actively executing.                                           |
# MAGIC | `SKIPPED`                   | The run was skipped (e.g., due to max concurrent runs reached).          |
# MAGIC | `TERMINATED`                | The run has finished, regardless of outcome.                             |
# MAGIC | `INTERNAL_ERROR`            | The run failed due to an internal system error.                          |
# MAGIC | `CANCELING`                 | A cancellation request has been issued; cleanup in progress.             |
# MAGIC | `CANCELED`                  | The run was canceled before completion.                                  |
# MAGIC | `TIMED_OUT`                 | The run exceeded its maximum allowed execution time.                     |
# MAGIC
# MAGIC #### `result_state`
# MAGIC
# MAGIC | result_state                    | definition                                                        |
# MAGIC |-----------------------------------|-----------------------------------------------------------------------|
# MAGIC | `SUCCESS`                         | All leaf tasks completed successfully.                                |
# MAGIC | `MAXIMUM_CONCURRENT_RUNS_REACHED` | The run was skipped due to exceeding the concurrent run limit.        |
# MAGIC | `CANCELED`                        | The run was canceled by a user or API call.                           |
# MAGIC | `FAILED`                          | One or more leaf tasks failed during execution.                       |
# MAGIC | `SKIPPED`                         | The run was skipped based on job skip logic or resource constraints.  |
# MAGIC | `TIMED_OUT`                       | The run timed out before finishing.                                   |
# MAGIC | `n/a`                             | The run is still in progress (no final outcome yet).    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementation

# COMMAND ----------

# DBTITLE 1,Dependencies
# Standard library imports
import os
import sys
import yaml
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

# Third-party imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    StringType,
    LongType,
    DateType,
)
from pyspark.sql.functions import col, upper, desc, row_number, concat_ws, lit, max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Local application imports
from utils.colorConfig import C
from utils.messageConfig import ClusterInfo
from utils.yamlConfig import YamlConfig
from utils.deltaConfig import DeltaTableUpdater
from utils.databricksAPIConfig import DatabricksAPIClient
from utils.environmentConfig import EnvironmentConfig
from utils.tokenConfig import TokenConfig

# COMMAND ----------

# DBTITLE 1,JobRunProcessor
# =============================================================================
# JobRunProcessor class
# =============================================================================
class JobRunProcessor:
    """
    Processes job run details by flattening the API response according to a defined schema.
    Caches job details to avoid redundant API calls.
    
    Note: The time_zone value is now passed from the environment and used for all time conversions.
    """
    def __init__(self, api_client: Any, schema: Dict[str, Any], yaml_config: Any, time_zone: Optional[str] = "UTC") -> None:
        # Save the API client, configuration, and time zone.
        self.api_client = api_client
        self.yaml_config = yaml_config
        self.job_details_cache: Dict[str, Dict[str, Any]] = {}
        self.column_mapping: Dict[str, Dict[str, Any]] = {}
        self.time_zone = time_zone  # Use the passed time zone (e.g., "UTC" or "America/Chicago")
        
        # Build mapping for each column based on the provided schema groups.
        for group in ["business", "plumbing"]:
            for col_def in schema.get(group, []):
                self.column_mapping[col_def["name"]] = {
                    "api_field": col_def.get("api_field", col_def["name"]),
                    "default": col_def.get("default", None),
                    "type": col_def["type"],
                }

    def _convert_time(self, epoch_ms: Optional[int], tz: str) -> Optional[datetime]:
        """
        Convert epoch milliseconds to a datetime object using the given time zone.
        Instead of hardcoding UTC, we now convert using ZoneInfo(tz).
        """
        if epoch_ms:
            return datetime.fromtimestamp(epoch_ms / 1000, tz=ZoneInfo(tz))
        return None

    def flatten_run_details(self, run_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten the nested run details into a dictionary based on the column mapping.
        Also extracts and processes schedule details.
        """
        flat: Dict[str, Any] = {}

        # Process basic fields.
        for final_col, mapping in self.column_mapping.items():
            api_field = mapping["api_field"]
            default_val = mapping.get("default")
            if final_col == "job_name":
                value = run_details.get("run_name") or run_details.get("job_name") or default_val or "n/a"
            elif final_col == "job_id":
                value = run_details.get("job_id") or run_details.get("job", {}).get("job_id", default_val or "n/a")
            elif api_field in ["life_cycle_state", "result_state", "error_code"]:
                value = run_details.get("state", {}).get(api_field, default_val or "n/a")
            else:
                value = run_details.get(api_field, default_val or "n/a")
            if value is None or (isinstance(value, str) and value.strip().lower() == "n/a"):
                value = default_val
            if mapping["type"] in ["IntegerType", "LongType"]:
                try:
                    value = int(value)
                except Exception:
                    value = default_val
            flat[final_col] = value

        # Process run_as and timestamps.
        if not flat.get("run_as") or flat.get("run_as").strip().lower() in ["", "n/a"]:
            flat["run_as"] = run_details.get("creator_user_name", "n/a")
        # Instead of forcing "UTC", we use the stored time zone variable.
        tz = self.time_zone
        timing = run_details.get("timing", {})
        raw_start = timing.get("start_time") or run_details.get("start_time")
        raw_end = timing.get("end_time") or run_details.get("end_time")
        flat["start_time_raw"] = raw_start if raw_start is not None else None
        flat["start_time"] = self._convert_time(raw_start, tz)
        flat["end_time"] = self._convert_time(raw_end, tz)
        if raw_start and raw_end:
            computed_duration = (raw_end - raw_start) / 1000.0
            flat["duration"] = f"{computed_duration} seconds"
        else:
            flat["duration"] = "n/a"
        queue = timing.get("queue_duration")
        flat["queue_duration"] = f"{queue} ms" if queue not in [None, ""] else "n/a"
        flat["description"] = run_details.get("description") or "n/a"
        flat["run_page_url"] = run_details.get("run_page_url") or "n/a"

        # Retrieve job details (if available) to extract settings.
        job_id = flat.get("job_id")
        if job_id and job_id != "n/a":
            if job_id not in self.job_details_cache:
                try:
                    job_details = self.api_client.get_job_details(job_id)
                except Exception as e:
                    print(f"{C.red}Error retrieving job details for job_id {job_id}: {e}{C.b}")
                    job_details = {}
                self.job_details_cache[job_id] = job_details
            else:
                job_details = self.job_details_cache[job_id]
        else:
            job_details = {}
        settings = job_details.get("settings", {})

        # Process tags.
        tags_dict = settings.get("tags", {})
        flat["tags"] = "|".join(f"{k}_{v}" for k, v in tags_dict.items()) if isinstance(tags_dict, dict) and tags_dict else "n/a"

        # Process email notifications.
        email_notif_raw = settings.get("email_notifications")
        formatted_email_notif_list = []
        formatted_notif_from_email = []
        if isinstance(email_notif_raw, dict):
            for k, v in email_notif_raw.items():
                val = "_".join(v) if isinstance(v, list) else str(v)
                if "@" in val:
                    formatted_email_notif_list.append(f"{k}-{val}")
                else:
                    formatted_notif_from_email.append(f"{k}-{val}")
        formatted_email_notif = "|".join(formatted_email_notif_list) if formatted_email_notif_list else "n/a"
        flat["email_notification"] = formatted_email_notif

        # Process notification settings.
        notif_settings_raw = settings.get("notification_settings")
        formatted_notif_settings_list = []
        if isinstance(notif_settings_raw, dict):
            for k, v in notif_settings_raw.items():
                formatted_notif_settings_list.append(f"{k}-{v}")
        formatted_notif_settings_list += formatted_notif_from_email
        formatted_notif_settings = "|".join(formatted_notif_settings_list) if formatted_notif_settings_list else "n/a"
        flat["notification_settings"] = formatted_notif_settings

        # --- Schedule extraction logic ---
        default_schedule_config = self.yaml_config.config.get("schedule_defaults", {})
        default_schedule_type = default_schedule_config.get("schedule_type", "n/a")
        default_schedule = default_schedule_config.get("schedule", "n/a")
        default_pause_status = default_schedule_config.get("pause_status", "n/a")
        sched = run_details.get("schedule", {})
        if isinstance(sched, dict) and sched:
            flat["timezone_id"] = sched.get("timezone_id") or tz
            if "quartz_cron_expression" in sched:
                flat["schedule_type"] = "quartz_cron_expression"
                flat["schedule"] = sched["quartz_cron_expression"]
            else:
                flat["schedule_type"] = sched.get("schedule_type") or default_schedule_type
                flat["schedule"] = sched.get("schedule") or default_schedule
            flat["pause_status"] = sched.get("pause_status") or default_pause_status
        else:
            flat["timezone_id"] = tz
            flat["schedule_type"] = default_schedule_type
            flat["schedule"] = default_schedule
            flat["pause_status"] = default_pause_status
        # --- End schedule extraction ---

        # Set modification and write timestamps using the configured time zone.
        dt_now = datetime.now(ZoneInfo(self.time_zone))
        flat["start_date"] = flat["start_time"].date() if flat.get("start_time") else None

        # --- Set plumbing columns using YAML defaults ---
        plumbing_defaults = {
            col_def.get("name"): col_def.get("default")
            for col_def in self.yaml_config.config.get("schema", {}).get("plumbing", [])
        }
        flat["created_at"] = dt_now
        flat["created_by"] = plumbing_defaults.get("created_by")
        flat["updated_at"] = plumbing_defaults.get("updated_at")
        flat["updated_by"] = plumbing_defaults.get("updated_by")

        return flat

    def build_row_for_run(self, run_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Public method to build a flattened record from raw API run details.
        """
        return self.flatten_run_details(run_details)

# COMMAND ----------

# DBTITLE 1,JobMonitor
# =============================================================================
# JobMonitor class
# =============================================================================
class JobMonitor:
    """
    Monitors job runs by fetching API data, processing new and pending runs, and merging updates into a Delta table.
    The time_zone for processing is set based on the environment variable.
    """
    def __init__(
        self,
        databricks_instance: str,
        token: str,
        table_name: str,
        yaml_config: YamlConfig,
        spark_session: Optional[SparkSession] = None,
        api_config: Dict[str, Any] = {},
        time_zone: Optional[str] = None,
    ) -> None:
        self.spark: SparkSession = spark_session if spark_session else SparkSession.builder.getOrCreate()
        self.api_client = DatabricksAPIClient(databricks_instance, token, api_config)
        self.yaml_config = yaml_config
        schema = self.yaml_config.config.get("schema", {})
        # Set the processing time zone to the passed value or default to "UTC".
        self.time_zone = time_zone if time_zone is not None else "UTC"
        # Pass the time_zone to JobRunProcessor.
        self.processor = JobRunProcessor(self.api_client, schema, self.yaml_config, time_zone=self.time_zone)
        self.table_updater = DeltaTableUpdater(self.spark, table_name, yaml_config=self.yaml_config)

    def get_existing_runs_df(self) -> DataFrame:
        target_cols = self.table_updater.schema.fieldNames()
        df: DataFrame = self.table_updater.read_table_as_df().select(*target_cols)
        df.cache()
        return df

    def get_pending_runs_df(self, existing_df: DataFrame) -> DataFrame:
        return existing_df.filter(upper(col("state")).isin("RUNNING", "PENDING", "QUEUED"))

    def update_pending_runs_sql(self, pending_df: DataFrame) -> None:
        """
        For each pending record, fetch updated API details and update the record using SQL.
        This method updates updated_at and updated_by based on the flattened row.
        When updating, updated_by is always set to the default value from YAML.
        """
        records = pending_df.collect()
        table_name = self.table_updater.table_name
        plumbing_defaults = {
            col_def.get("name"): col_def.get("default")
            for col_def in self.yaml_config.config.get("schema", {}).get("plumbing", [])
        }
        default_updated_by = plumbing_defaults.get("created_by", "AdminUser")
        for record in records:
            run_id = record.run_id
            try:
                updated_run = self.api_client.get_run_details(run_id)
                updated_row = self.processor.build_row_for_run(updated_run)
                # Use the configured time zone for getting the current time.
                utc_now = datetime.now(ZoneInfo(self.time_zone))
                utc_now_str = utc_now.strftime("%Y-%m-%d %H:%M:%S")
                state_upper = updated_row.get("state", "n/a").upper()
                if state_upper not in {"RUNNING", "PENDING", "QUEUED"}:
                    update_query = f"""
                        UPDATE {table_name}
                        SET state = '{updated_row.get("state", "n/a")}',
                            duration = '{updated_row.get("duration", "n/a")}',
                            updated_at = timestamp'{utc_now_str}',
                            updated_by = '{default_updated_by}'
                        WHERE {self._primary_key_condition(record)}
                    """
                else:
                    if record.start_time_raw is not None:
                        # Use the designated time zone for current time.
                        current_time_ms = int(datetime.now(ZoneInfo(self.time_zone)).timestamp() * 1000)
                        duration_seconds = (current_time_ms - record.start_time_raw) / 1000.0
                        computed_duration = f"{duration_seconds:.3f} seconds"
                    else:
                        computed_duration = updated_row.get("duration", "n/a")
                    update_query = f"""
                        UPDATE {table_name}
                        SET duration = '{computed_duration}',
                            updated_at = timestamp'{utc_now_str}',
                            updated_by = '{default_updated_by}'
                        WHERE {self._primary_key_condition(record)}
                    """
                print("Executing update query:")
                print(update_query)
                self.spark.sql(update_query)
                print("Record updated.")
            except Exception as e:
                print(f"{C.red}Error updating run {record.run_id}: {e}{C.b}")

    def _primary_key_condition(self, record: Any) -> str:
        """
        Build the SQL condition for matching the primary key.
        Uses the value from dl_primary_key_column defined in YAML.
        """
        primary_key = self.yaml_config.config.get("app_config", {}) \
                        .get("plumbing_columns", {}) \
                        .get("dl_primary_key_column")
        if not primary_key:
            raise Exception("Primary key (dl_primary_key_column) is not defined in the YAML config.")
        pk_value = getattr(record, primary_key)
        return f"{primary_key} = '{pk_value}'"

    def process_new_api_runs(self, api_runs: List[Dict[str, Any]]) -> DataFrame:
        target_cols = self.table_updater.schema.fieldNames()
        def process_run(run: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            try:
                new_row = self.processor.build_row_for_run(run)
                return new_row
            except Exception as e:
                print(f"{C.red}Error processing run {run.get('run_id')}: {e}{C.b}")
                return None
        processed = [row for row in (process_run(run) for run in api_runs) if row is not None]
        new_runs_df = self.spark.createDataFrame(processed, schema=self.table_updater.schema).select(*target_cols)
        return new_runs_df

    def _determine_cutoff(self, existing_df: DataFrame) -> Tuple[bool, int]:
        num_records = existing_df.count()
        if num_records == 0:
            full_load = True
            historical_date_str = self.yaml_config.config.get("app_config", {}).get("historical_load_date")
            if not historical_date_str:
                raise Exception("historical_load_date must be specified in the YAML configuration for full loads.")
            historical_date = datetime.fromisoformat(historical_date_str)
            cutoff = int(historical_date.timestamp() * 1000)
            print(f"{C.ivory}Table empty (full load). Using YAML historical_load_date '{historical_date_str}' as cutoff: {C.bubblegum_pink}{cutoff}{C.ivory}.{C.b}")
        else:
            full_load = False
            max_start_time = existing_df.agg(spark_max("start_time_raw")).collect()[0][0]
            cutoff = max_start_time
            print(f"{C.ivory}Table has data. Using max(start_time_raw) as cutoff: {C.bubblegum_pink}{cutoff}{C.ivory}.{C.b}")
        return full_load, cutoff

    def _fetch_api_runs(self, cutoff: int) -> List[Dict[str, Any]]:
        api_runs = self.api_client.get_all_runs_since(cutoff)
        print(f"{C.ivory}Fetched {C.bubblegum_pink}{len(api_runs)}{C.ivory} runs from API since cutoff.{C.b}")
        return api_runs

    def _merge_updates(self, existing_df: DataFrame, updated_pending_df: Optional[DataFrame],
                       new_runs_df: DataFrame) -> None:
        target_cols = self.table_updater.schema.fieldNames()
        primary_key = self.yaml_config.config.get("app_config", {}) \
                        .get("plumbing_columns", {}) \
                        .get("dl_primary_key_column")
        if not primary_key:
            raise Exception("Primary key (dl_primary_key_column) is not defined in the YAML config.")
        business_key_cols = [primary_key]

        if updated_pending_df is None:
            combined_df = new_runs_df
        else:
            terminal_df = existing_df.filter(~upper(col("state")).isin("RUNNING", "PENDING", "QUEUED")).select(*target_cols)
            combined_df = terminal_df.unionByName(updated_pending_df).unionByName(new_runs_df)

        windowSpec = Window.partitionBy(*business_key_cols).orderBy(desc("updated_at"))
        deduped_df = combined_df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).drop("rn")
        merge_count = deduped_df.count()
        print(f"{C.ivory}Merging {C.bubblegum_pink}{merge_count}{C.ivory} deduplicated records into the Delta table.")
        print(f"{C.b}{C.moccasin}===== MERGING RECORDS INTO DELTA TABLE ====={C.b}")
        self.table_updater.merge_df(deduped_df, f"target.{primary_key} = source.{primary_key}")
        print(f"{C.b}{C.moccasin}===== END MERGE ====={C.b}")

    def _validate(self) -> None:
        target_cols = self.table_updater.schema.fieldNames()
        final_df: DataFrame = self.table_updater.read_table_as_df().select(*target_cols)
        final_count = final_df.count()
        print(f"{C.ivory}Final row count after merge: {C.bubblegum_pink}{final_count}{C.ivory}.{C.b}")
        print(f"{C.b}{C.moccasin}=== VALIDATION CHECKS CONCLUDED ==={C.b}\n")

    def update_job_runs(self) -> None:
        target_cols = self.table_updater.schema.fieldNames()
        print(f"{C.b}{C.aqua_blue}===== STARTING update_job_runs PROCEDURE ====={C.b}")
        existing_df: DataFrame = self.table_updater.read_table_as_df().select(*target_cols)
        existing_df.cache()
        existing_run_ids = [row.run_id for row in existing_df.select("run_id").distinct().collect()]
        num_records = existing_df.count()
        print(f"{C.b}{C.ivory}Found {C.bubblegum_pink}{len(existing_run_ids)}{C.ivory} unique run IDs and {C.bubblegum_pink}{num_records}{C.ivory} records in {C.candy_red}{self.table_updater.table_name}{C.ivory}.{C.b}")

        print(f"{C.b}{C.moccasin}===== SECTION 2: DETERMINE CUTOFF TIMESTAMP ====={C.b}")
        full_load, cutoff = self._determine_cutoff(existing_df)
        print(f"{C.b}{C.moccasin}===== END SECTION 2 ====={C.b}\n")

        print(f"{C.b}{C.moccasin}===== SECTION 3: FETCH RUNS FROM API ====={C.b}")
        api_runs = self._fetch_api_runs(cutoff)
        print(f"{C.b}{C.moccasin}===== END SECTION 3 ====={C.b}\n")

        print(f"{C.b}{C.moccasin}===== SECTION 4: PROCESS NEW RUNS ====={C.b}")
        new_runs_df = self.process_new_api_runs(api_runs)
        new_run_ids = [row.run_id for row in new_runs_df.select("run_id").distinct().collect()]
        matching_run_ids = list(set(existing_run_ids).intersection(set(new_run_ids)))
        unique_new_runs = list(set(new_run_ids) - set(existing_run_ids))
        print(f"{C.ivory}Found {C.bubblegum_pink}{len(matching_run_ids)}{C.ivory} matching and {C.bubblegum_pink}{len(unique_new_runs)}{C.ivory} unique API runs.{C.b}")
        print(f"{C.ivory}Processed {C.bubblegum_pink}{new_runs_df.count()}{C.ivory} unique new runs.{C.b}")
        print(f"{C.b}{C.moccasin}===== END SECTION 4 ====={C.b}\n")

        print(f"{C.b}{C.moccasin}===== SECTION 5: UPDATE PENDING RUNS ====={C.b}")
        if not full_load:
            pending_df = self.get_pending_runs_df(existing_df).select(*target_cols)
            pending_count = pending_df.count()
            print(f"{C.ivory}Found {C.bubblegum_pink}{pending_count}{C.ivory} non-terminal records to update.{C.b}")
            if pending_count > 0:
                self.update_pending_runs_sql(pending_df)
                updated_pending_df = self.get_pending_runs_df(self.table_updater.read_table_as_df().select(*target_cols))
            else:
                updated_pending_df = pending_df
            print(f"{C.b}{C.moccasin}===== END SECTION 5 ====={C.b}\n")
        else:
            print(f"{C.ivory}Full load: skipping pending updates.{C.b}")
            print(f"{C.b}{C.moccasin}===== END SECTION 5 ====={C.b}\n")
            updated_pending_df = None

        print(f"{C.b}{C.moccasin}===== SECTION 6: MERGE UPDATES INTO DELTA TABLE ====={C.b}")
        self._merge_updates(existing_df, updated_pending_df, new_runs_df)
        print(f"{C.b}{C.moccasin}===== END SECTION 6 ====={C.b}\n")

        print(f"{C.b}{C.moccasin}=== CONDUCTING VALIDATION CHECKS ==={C.b}")
        self._validate()
        print(f"{C.b}{C.aqua_blue}===== update_job_runs PROCEDURE COMPLETED ====={C.b}\n")

# COMMAND ----------

# DBTITLE 1,Execution Logic
if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    # EnvironmentConfig returns a tuple (ENV, TIMEZONE, LOCAL_TIMEZONE)
    env, processing_timezone, local_timezone = EnvironmentConfig().environment

    # Instantiate YamlConfig with job-monitor.yaml.
    yaml_config = YamlConfig("yaml/job-monitor.yaml", env=env, spark=spark)

    # Instantiate TokenConfig with token-management.yaml and the given environment.
    databricks_instance, token = TokenConfig("yaml/token-management.yaml", env=env, spark=spark)

    # Print cluster and notebook information.
    cluster_info = ClusterInfo(spark, databricks_instance, token, local_timezone, print_env_var=True)

    # Instantiate JobMonitor and run the update procedure.
    monitor = JobMonitor(
         databricks_instance,
         token,
         yaml_config.full_table_name,
         yaml_config=yaml_config,
         spark_session=spark,
         api_config=yaml_config.config.get("app_config", {}).get("api", {}),
         time_zone=processing_timezone,
    )
    try:
         monitor.update_job_runs()
    except Exception as e:
         print(f"Error updating job runs: {e}")
