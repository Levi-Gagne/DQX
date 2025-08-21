# src/dqx/utils/runtime.py

from __future__ import annotations

import sys
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from utils.color import Color
from utils.display import show_df
from utils.timezone import current_time_iso

# Safe in notebooks/jobs; Spark Connect/unit tests tolerate failure
try:
    from databricks.sdk.runtime import dbutils  # type: ignore
except Exception:
    dbutils = None  # type: ignore


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


# ---------- single public API ----------
def show_notebook_env(
    spark: SparkSession,
    *,
    local_timezone: Optional[str] = None,
    format: str = "printed",
) -> Dict[str, Any]:
    """
    Show Databricks/Spark runtime details in the requested format.

    Args:
        spark: active SparkSession
        local_timezone: IANA TZ name for the "local_time" label (falls back to UTC if unknown)
        format: "printed" for a colorized text block, or "table" to render a single-row DataFrame

    Returns:
        A dict with the same fields that are displayed.
    """
    # Build the info dict inline (no separate gather_* function)
    info: Dict[str, Any] = {
        "python_version":  _get_python_version(),
        "spark_version":   _get_spark_version(spark),
        "notebook_path":   _get_notebook_path(),
        "cluster_name":    _get_cluster_name(spark),
        "cluster_id":      _get_cluster_id(spark),
        "executor_memory": _get_executor_memory(spark),
        "local_time":      current_time_iso(local_timezone),  # never raises; falls back to UTC
        "utc_time":        current_time_iso("UTC"),
    }

    fmt = (format or "printed").strip().lower()

    if fmt == "table":
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
        return info

    # Default: colorized printed block
    try:
        sep = f"{Color.saffron}={Color.r}"
        print(sep * 60)
        print(f"{Color.b}{Color.python}Python version    :{Color.r} {Color.b}{Color.ivory}{info['python_version']}{Color.r}")
        print(f"{Color.b}{Color.spark}Spark version     :{Color.r} {Color.b}{Color.ivory}{info['spark_version']}{Color.r}")
        print(f"{Color.b}{Color.forest_green}Notebook path     :{Color.r} {Color.b}{Color.ivory}{info['notebook_path']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster name      :{Color.r} {Color.b}{Color.ivory}{info['cluster_name']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster ID        :{Color.r} {Color.b}{Color.ivory}{info['cluster_id']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Executor memory   :{Color.r} {Color.b}{Color.ivory}{info['executor_memory']}{Color.r}")
        tz_label = local_timezone or "UTC"
        print(f"{Color.b}{Color.neon_blue}Local time ({tz_label}):{Color.r} {Color.b}{Color.ivory}{info['local_time']}{Color.r}")
        print(f"{Color.b}{Color.neon_blue}UTC time          :{Color.r} {Color.b}{Color.ivory}{info['utc_time']}{Color.r}")
        print(sep * 60)
    except Exception as e:
        # Never fail the job for cosmetics
        print(f"[runtime] Non-fatal while printing runtime info: {e}")

    return info


__all__ = ["show_notebook_env"]