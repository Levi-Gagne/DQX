# src/dqx/utils/runtime.py

from __future__ import annotations

import sys
from typing import Dict, Any
from pyspark.sql import SparkSession
from utils.color import Color

# Safe in notebooks/jobs; Spark Connect/unit tests tolerate failure
try:
    from databricks.sdk.runtime import dbutils  # type: ignore
except Exception:
    dbutils = None  # type: ignore

def current_time_iso(time_zone: Optional[str] = "UTC") -> str:
    """
    Return the current datetime as an ISO 8601 string with timezone.
    """
    try:
        zone = ZoneInfo(time_zone)
    except Exception as e:
        raise ValueError(
            f"Invalid timezone '{time_zone}'. Must be a valid IANA timezone string. "
            "Examples: 'UTC', 'America/Chicago', 'Europe/Berlin'."
        ) from e
    return datetime.now(zone).isoformat()

# ---------- internals ----------
def _safe_conf_get(spark: SparkSession, key: str, default: str = "Unknown") -> str:
    try:
        return spark.conf.get(key)
    except Exception:
        return default

def _get_spark_version(spark: SparkSession) -> str:
    return spark.version

def _get_python_version() -> str:
    return sys.version.split()[0]

def _get_cluster_id(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.databricks.clusterUsageTags.clusterId", "Unknown")

def _get_cluster_name(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.databricks.clusterUsageTags.clusterName", "Unknown")

def _get_executor_memory(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.executor.memory", "Unknown")

def _get_notebook_path() -> str:
    try:
        if dbutils is None:
            return "Unknown"
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return "Unknown"

def _get_current_times(local_timezone: str = "America/Chicago") -> Dict[str, str]:
    return {
        "local_time": current_time_iso(local_timezone),
        "utc_time": current_time_iso("UTC"),
    }

# ---------- public API ----------
def gather_runtime_info(spark: SparkSession, *, local_timezone: str = "America/Chicago") -> Dict[str, Any]:
    """Return a structured dict of runtime facts (no printing, no DataFrame)."""
    return {
        "python_version":  _get_python_version(),
        "spark_version":   _get_spark_version(spark),
        "notebook_path":   _get_notebook_path(),
        "cluster_name":    _get_cluster_name(spark),
        "cluster_id":      _get_cluster_id(spark),
        "executor_memory": _get_executor_memory(spark),
        **_get_current_times(local_timezone),
    }

def print_notebook_env(spark: SparkSession, local_timezone: str = "America/Chicago") -> Dict[str, Any]:
    """
    Print Databricks/Spark runtime details as a COLORED TEXT BLOCK (not a table).
    Returns the same info dict.
    """
    try:
        info = gather_runtime_info(spark, local_timezone=local_timezone)
        print(f"{Color.saffron}={Color.r}" * 60)
        print(f"{Color.b}{Color.python}Python version    :{Color.r} {Color.b}{Color.ivory}{info['python_version']}{Color.r}")
        print(f"{Color.b}{Color.spark}Spark version     :{Color.r} {Color.b}{Color.ivory}{info['spark_version']}{Color.r}")
        print(f"{Color.b}{Color.forest_green}Notebook path     :{Color.r} {Color.b}{Color.ivory}{info['notebook_path']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster name      :{Color.r} {Color.b}{Color.ivory}{info['cluster_name']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster ID        :{Color.r} {Color.b}{Color.ivory}{info['cluster_id']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Executor memory   :{Color.r} {Color.b}{Color.ivory}{info['executor_memory']}{Color.r}")
        print(f"{Color.b}{Color.neon_blue}Local time ({local_timezone}):{Color.r} {Color.b}{Color.ivory}{info['local_time']}{Color.r}")
        print(f"{Color.b}{Color.neon_blue}UTC time          :{Color.r} {Color.b}{Color.ivory}{info['utc_time']}{Color.r}")
        print(f"{Color.saffron}={Color.r}" * 60)
        return info
    except Exception as e:
        print(f"[runtime] Non-fatal while printing runtime info: {e}")
        return {"python_version": _get_python_version(), "spark_version": spark.version}

# Optional: if you ever WANT a table version of the env block (call explicitly)
def show_notebook_env_table(spark: SparkSession, local_timezone: str = "America/Chicago") -> None:
    info = gather_runtime_info(spark, local_timezone=local_timezone)
    from utils.display import show_df  # local import to avoid any coupling at import-time
    df = spark.createDataFrame(
        [(
            info["python_version"],
            info["spark_version"],
            info["notebook_path"],
            info["cluster_name"],
            info["cluster_id"],
            info["executor_memory"],
            info["local_time"],
            info["utc_time"],
        )],
        schema=(
            "python_version string, spark_version string, notebook_path string, "
            "cluster_name string, cluster_id string, executor_memory string, "
            "local_time string, utc_time string"
        ),
    )
    show_df(df, n=1, truncate=False)

__all__ = ["gather_runtime_info", "print_notebook_env", "show_notebook_env_table"]