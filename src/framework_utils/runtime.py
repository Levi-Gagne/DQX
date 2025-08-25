# src/dqx/utils/runtime.py

from __future__ import annotations
import sys
from typing import Dict, Any, Optional
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

from pyspark.sql import SparkSession
from framework_utils.color import Color
from framework_utils.display import show_df

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

def _now_iso_in(tz_name: Optional[str]) -> str:
    """Python-only clock. Prefer tz_name via zoneinfo; fallback to UTC. Never touches Spark."""
    now_utc = datetime.now(timezone.utc)
    if tz_name and ZoneInfo is not None:
        try:
            return now_utc.astimezone(ZoneInfo(tz_name)).isoformat()
        except Exception:
            pass
    return now_utc.isoformat()


# ---------- public API ----------
def show_notebook_env(
    spark: SparkSession,
    *,
    local_timezone: Optional[str] = None,   # default below = America/Chicago
    format: str = "printed",
    show_spark_sql_tz: bool = True,         # display Spark session TZ without changing it
) -> Dict[str, Any]:
    """
    Display runtime details. This function **never sets** Spark configs.
    - Local time is computed via Python (zoneinfo), defaulting to America/Chicago.
    - UTC time is computed via Python (datetime.timezone.utc).
    - Optionally shows current Spark SQL session TZ as an FYI.
    """
    tz_local = (local_timezone or "America/Chicago").strip()
    spark_sql_tz = _safe_conf_get(spark, "spark.sql.session.timeZone", "Unknown") if show_spark_sql_tz else "Hidden"

    info: Dict[str, Any] = {
        "python_version":     _get_python_version(),
        "spark_version":      _get_spark_version(spark),
        "notebook_path":      _get_notebook_path(),
        "cluster_name":       _get_cluster_name(spark),
        "cluster_id":         _get_cluster_id(spark),
        "executor_memory":    _get_executor_memory(spark),
        "spark_sql_timezone": spark_sql_tz,
        "local_time":         _now_iso_in(tz_local),
        "utc_time":           _now_iso_in("UTC"),
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
                info["spark_sql_timezone"],
                info["local_time"],
                info["utc_time"],
            )],
            schema=(
                "python_version string, spark_version string, notebook_path string, "
                "cluster_name string, cluster_id string, executor_memory string, "
                "spark_sql_timezone string, local_time string, utc_time string"
            ),
        )
        show_df(df, n=1, truncate=False)
        return info

    # printed
    try:
        sep = f"{Color.saffron}={Color.r}"
        print(sep * 60)
        print(f"{Color.b}{Color.python}Python version       :{Color.r} {Color.b}{Color.ivory}{info['python_version']}{Color.r}")
        print(f"{Color.b}{Color.spark}Spark version        :{Color.r} {Color.b}{Color.ivory}{info['spark_version']}{Color.r}")
        print(f"{Color.b}{Color.forest_green}Notebook path        :{Color.r} {Color.b}{Color.ivory}{info['notebook_path']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster name         :{Color.r} {Color.b}{Color.ivory}{info['cluster_name']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Cluster ID           :{Color.r} {Color.b}{Color.ivory}{info['cluster_id']}{Color.r}")
        print(f"{Color.b}{Color.CLUSTER_SECONDARY}Executor memory      :{Color.r} {Color.b}{Color.ivory}{info['executor_memory']}{Color.r}")
        if show_spark_sql_tz:
            print(f"{Color.b}{Color.neon_blue}Spark SQL time zone  :{Color.r} {Color.b}{Color.ivory}{info['spark_sql_timezone']}{Color.r}")
        print(f"{Color.b}{Color.neon_blue}Local time ({tz_local}):{Color.r} {Color.b}{Color.ivory}{info['local_time']}{Color.r}")
        print(f"{Color.b}{Color.neon_blue}UTC time             :{Color.r} {Color.b}{Color.ivory}{info['utc_time']}{Color.r}")
        print(sep * 60)
    except Exception as e:
        print(f"[runtime] Non-fatal while printing runtime info: {e}")

    return info


__all__ = ["show_notebook_env"]