# utils/print.py

import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

from databricks.sdk.runtime import dbutils

from utils.timezone import current_time_iso

def get_spark_version(spark: SparkSession) -> str:
    """Return the current Spark version."""
    return spark.version

def get_python_version() -> str:
    """Return the current Python major.minor.patch version."""
    return sys.version.split()[0]

def get_cluster_id(spark: SparkSession) -> str:
    """Return the Databricks cluster ID from Spark config."""
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "Unknown")

def get_cluster_name(spark: SparkSession) -> str:
    """Return the Databricks cluster name from Spark config."""
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "Unknown")

def get_executor_memory(spark: SparkSession) -> str:
    """Return the Spark executor memory setting."""
    return spark.conf.get("spark.executor.memory", "Unknown")

def get_notebook_path() -> str:
    """Return the Databricks notebook path, or 'Unknown' if unavailable."""
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return "Unknown"

def get_current_times(local_timezone: str = "America/Chicago") -> Dict[str, str]:
    """
    Return ISO timestamps for local timezone and UTC.
    Uses your current_time_iso utility.
    """
    return {
        "local_time": current_time_iso(local_timezone),
        "utc_time": current_time_iso("UTC")
    }

def print_notebook_env(spark: SparkSession, local_timezone: str = "America/Chicago") -> Dict[str, Any]:
    """
    Print Databricks and Spark environment details. Returns all collected info as a dict.
    """
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
        "utc_time": times["utc_time"]
    }

# Usage example:
# spark = SparkSession.builder.getOrCreate()
# print_notebook_env(spark)