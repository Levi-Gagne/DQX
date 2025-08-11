# utils/print.py

import sys
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

# dbutils may not exist under Spark Connect / SQL Warehouse
try:
    from databricks.sdk.runtime import dbutils  # noqa: F401
except Exception:  # pragma: no cover
    dbutils = None  # type: ignore

from utils.timezone import current_time_iso


def _safe_conf_get(spark: SparkSession, key: str, default: str = "Unknown") -> str:
    """Never raise on CONFIG_NOT_AVAILABLE under Spark Connect."""
    try:
        # Some runtimes ignore the default and still raise; handle explicitly.
        return spark.conf.get(key)
    except Exception:
        return default


def get_spark_version(spark: SparkSession) -> str:
    return spark.version


def get_python_version() -> str:
    return sys.version.split()[0]


def get_cluster_id(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.databricks.clusterUsageTags.clusterId", "Unknown")


def get_cluster_name(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.databricks.clusterUsageTags.clusterName", "Unknown")


def get_executor_memory(spark: SparkSession) -> str:
    return _safe_conf_get(spark, "spark.executor.memory", "Unknown")


def get_notebook_path() -> str:
    try:
        if dbutils is None:
            return "Unknown"
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return "Unknown"


def get_current_times(local_timezone: str = "America/Chicago") -> Dict[str, str]:
    return {
        "local_time": current_time_iso(local_timezone),
        "utc_time": current_time_iso("UTC"),
    }


def print_notebook_env(spark: SparkSession, local_timezone: str = "America/Chicago") -> Dict[str, Any]:
    """
    Print Databricks and Spark environment details. Returns all collected info as a dict.
    Safe for clusters, jobs, and SQL Warehouses (Spark Connect).
    """
    try:
        spark_version = get_spark_version(spark)
        python_version = get_python_version()
        cluster_id = get_cluster_id(spark)
        cluster_name = get_cluster_name(spark)
        executor_memory = get_executor_memory(spark)
        notebook_path = get_notebook_path()
        times = get_current_times(local_timezone)

        print("=" * 60)
        print(f"Python version    : {python_version}")
        print(f"Spark version     : {spark_version}")
        print(f"Notebook path     : {notebook_path}")
        print(f"Cluster name      : {cluster_name}")
        print(f"Cluster ID        : {cluster_id}")
        print(f"Executor memory   : {executor_memory}")
        print(f"Local time ({local_timezone}): {times['local_time']}")
        print(f"UTC time          : {times['utc_time']}")
        print("=" * 60)

        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "executor_memory": executor_memory,
            "notebook_path": notebook_path,
            "spark_version": spark_version,
            "python_version": python_version,
            "local_time": times["local_time"],
            "utc_time": times["utc_time"],
        }
    except Exception as e:
        # Absolute last-resort fallback so your pipeline never dies on env printing.
        print(f"[env] Non-fatal while printing env: {e}")
        out = {"python_version": get_python_version(), "spark_version": spark.version}
        try:
            row = spark.sql(
                "select current_catalog() as catalog, current_schema() as schema, current_user() as user"
            ).first()
            out.update({"catalog": row.catalog, "schema": row.schema, "user": row.user})
            print(f"[env] catalog={row.catalog}, schema={row.schema}, user={row.user}")
        except Exception:
            pass
        # Try a few known keys individually, swallowing Connect errors.
        for k in (
            "spark.databricks.clusterUsageTags.clusterName",
            "spark.databricks.clusterUsageTags.clusterId",
            "spark.databricks.sql.initial.catalog.name",
        ):
            v = _safe_conf_get(spark, k, "N/A")
            print(f"[env] {k}={v}")
            out[k] = v
        return out