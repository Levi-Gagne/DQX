"""
Module: utils/messageConfig.py
Description:
    This module provides utilities for introspecting and displaying the execution environment
    within a Databricks workspace. It defines the ClusterInfo class, which gathers Spark,
    notebook, and cluster metadata, formats key‑value details for readability, and prints
    the information with color‑coded output. These utilities simplify environment diagnostics
    in both interactive notebooks and automated jobs.

Workflow:
    1. Initialization:
        - Accepts a SparkSession and, optionally, databricks_instance, token, local_timezone,
          and print_env_var flag.
        - If databricks_instance, token, and local_timezone are provided:
            * Calls get_notebook_info(local_timezone) to collect:
                - Cluster ID and name from Spark config tags
                - Notebook path via dbutils context
                - Executor memory setting
                - Current timestamp in the specified timezone
            * If print_env_var is True, instantiates EnvironmentConfig and prints ENV, TIMEZONE,
              and LOCAL_TIMEZONE.
            * Calls get_cluster_info(databricks_instance, token) to fetch and display full
              cluster details via REST API.

    2. Timestamp Conversion:
        - convert_timestamp(timestamp): Static method that converts an epoch‑millisecond timestamp
          to a human‑readable "YYYY‑MM‑DD HH:MM:SS" string or returns "N/A" if input is missing.

    3. Dictionary Formatting:
        - format_dict(d, indent, key_color): Static method that renders a dict as an indented,
          colored bullet list, improving readability of nested metadata.

    4. Notebook Info Retrieval:
        - get_notebook_info(local_timezone): Instance method that:
            * Reads clusterUsageTags.clusterId and .clusterName from Spark config.
            * Retrieves notebookPath from dbutils.
            * Reads spark.executor.memory setting.
            * Loads ZoneInfo for the specified timezone, falling back to UTC on error.
            * Captures current time and prints:
                - Python and Spark versions
                - Notebook path
                - Current timestamp with timezone
                - Cluster name, ID, and executor memory

    5. Cluster Info Retrieval:
        - get_cluster_info(databricks_instance, token): Instance method that:
            * Ensures cluster_id is populated (invokes get_notebook_info("UTC") if needed).
            * Constructs the URL for the /api/2.1/clusters/get endpoint.
            * Sends a GET request with bearer token authentication.
            * On HTTP 200, parses JSON and prints:
                - Cluster ID and creator user name
                - Driver node details (private IP, public DNS, start timestamp)
                - Executor node details (private IP, public DNS, start timestamp)
                - Other metadata: Spark version, state, memory, cores, autoscale settings,
                  node type IDs, and custom tags
            * On non‑200 response, logs the HTTP status code failure.

Usage:
    from utils.messageConfig import ClusterInfo
    spark = SparkSession.builder.getOrCreate()

    # Retrieve just notebook info:
    ci = ClusterInfo(spark)
    ci.get_notebook_info("America/Chicago")

    # Full diagnostics including environment vars and cluster details:
    ClusterInfo(
        spark,
        databricks_instance="mycompany.cloud.databricks.com",
        token="<your_token>",
        local_timezone="America/Chicago",
        print_env_var=True
    )

Note:
    - Requires `databricks.sdk.runtime.dbutils` to be available in the Databricks runtime.
    - Depends on the `requests` and `zoneinfo` libraries, as well as the custom
      `utils.colorConfig` and `utils.environmentConfig` modules.
    - Intended for diagnostics and monitoring within Databricks notebooks and workflows.

Author: Levi Gagne
Created Date: 2025-03-25
Last Modified: 2025-04-16
"""

import sys                                    # To retrieve Python runtime version
import requests                              # For HTTP calls to Databricks REST API
from datetime import datetime                # For timestamp formatting
from zoneinfo import ZoneInfo                # For timezone‐aware datetimes
from typing import Optional                  

from databricks.sdk.runtime import dbutils    # Databricks utility functions
from utils.colorConfig import C               # Custom color codes for styled terminal output


class ClusterInfo:
    """
    Retrieves and displays notebook and cluster information in a Databricks workspace.
    """
    def __init__(
        self,
        spark,
        databricks_instance: Optional[str] = None,
        token: Optional[str] = None,
        local_timezone: Optional[str] = None,
        print_env_var: bool = False
    ):
        """
        Initialize ClusterInfo and optionally execute full diagnostics.

        Parameters:
            spark: Active SparkSession.
            databricks_instance: Databricks workspace URL (no protocol).
            token: Bearer token for API authentication.
            local_timezone: IANA timezone string for timestamp conversion.
            print_env_var: If True, prints ENV, TIMEZONE, LOCAL_TIMEZONE before cluster info.
        """
        self.spark = spark
        self.cluster_id = None
        self.cluster_name = None
        self.executor_memory = None

        # If all optional parameters are provided, perform full diagnostics
        if databricks_instance and token and local_timezone:
            # Gather and print notebook & Spark info
            self.get_notebook_info(local_timezone)
            # Print environment variables if requested
            if print_env_var:
                from utils.environmentConfig import EnvironmentConfig
                EnvironmentConfig().print_environment_info()
            # Fetch and print detailed cluster info via REST API
            self.get_cluster_info(databricks_instance, token)

    @staticmethod
    def convert_timestamp(timestamp: Optional[int]) -> str:
        """
        Convert an epoch‐millisecond timestamp into 'YYYY‑MM‑DD HH:MM:SS'.

        Returns "N/A" if timestamp is None or zero.
        """
        if not timestamp:
            return "N/A"
        return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def format_dict(d: dict, indent: int = 8, key_color: str = C.CLUSTER_TERTIARY) -> str:
        """
        Render a dict as an indented, colored bullet list.

        Parameters:
            d: Dictionary of items to format.
            indent: Number of spaces before bullet.
            key_color: Color code to apply to keys.

        Returns:
            A multiline string with each key‐value pair on its own line.
        """
        lines = []
        for k, v in d.items():
            lines.append(f"{' ' * indent}- {C.b}{key_color}{k}:{C.r} {v}")
        return "\n".join(lines)

    def get_notebook_info(self, local_timezone: str):
        """
        Retrieve and print basic notebook and Spark environment details.

        Parameters:
            local_timezone: IANA timezone string for current time display.

        Returns:
            Tuple(cluster_id, cluster_name, executor_memory, notebook_path, current_time).
        """
        # Read cluster ID and name from Spark configuration tags
        self.cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        self.cluster_name = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
        # Obtain notebook path from dbutils context
        notebook_path = dbutils.notebook.entry_point.getDbutils() \
            .notebook().getContext().notebookPath().get()
        # Read configured executor memory
        self.executor_memory = self.spark.conf.get("spark.executor.memory", "Unknown")

        # Attempt to load the specified timezone; fall back to UTC on error
        try:
            tzinfo = ZoneInfo(local_timezone)
        except Exception as e:
            print(f"{C.b}{C.ivory}Warning: Unable to load timezone '{local_timezone}': {e}. Falling back to UTC.{C.r}")
            tzinfo = ZoneInfo("UTC")
        current_time = datetime.now(tzinfo)

        # Print collected details with color formatting
        print(f"{C.b}{C.python}Python Version:{C.r} {sys.version.split()[0]}")
        print(f"{C.b}{C.spark}Spark Version:{C.r} {C.b}{C.ghost_white}{self.spark.version}{C.r}")
        print(f"{C.b}{C.fuchsia}Notebook Path:{C.r} {C.b}{C.ghost_white}{notebook_path}{C.r}")
        print(f"{C.b}{C.cool_gray}Current Time:{C.r} "
              f"{C.b}{C.ghost_white}{current_time.strftime('%Y-%m-%d %H:%M:%S')} {current_time.tzinfo}{C.r}")
        print(f"{C.b}{C.moccasin}Cluster Name:{C.r} {C.b}{C.ghost_white}{self.cluster_name}{C.r}")
        print(f"{C.b}{C.moccasin}Cluster ID:{C.r} {C.b}{C.ghost_white}{self.cluster_id}{C.r}")
        print(f"{C.b}{C.moccasin}Executor Memory:{C.r} {C.b}{C.ghost_white}{self.executor_memory}{C.r}")

        return self.cluster_id, self.cluster_name, self.executor_memory, notebook_path, current_time

    def get_cluster_info(self, databricks_instance: str, token: str):
        """
        Retrieve and print detailed cluster configuration via Databricks REST API.

        Parameters:
            databricks_instance: Workspace URL including protocol.
            token: Bearer token for API authentication.
        """
        # Ensure notebook info exists
        if not self.cluster_id:
            self.get_notebook_info("UTC")

        # Build API request for cluster details
        url = f"{databricks_instance}/api/2.1/clusters/get"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"cluster_id": self.cluster_id}

        response = requests.get(url, headers=headers, json=payload)
        if response.status_code == 200:
            cluster_info = response.json()

            # Print cluster header
            print(f"\n{C.b}{C.CLUSTER_PRIMARY}Cluster: {C.r}"
                  f"{C.b}{C.CLUSTER_QUATERNARY}{self.cluster_name}{C.r}")
            print(f"{C.CLUSTER_PRIMARY}" + "-" * 60 + f"{C.r}")

            # Print metadata
            print(f"{C.b}{C.CLUSTER_PRIMARY}Cluster ID: {C.r}"
                  f"{C.b}{C.CLUSTER_QUATERNARY}{cluster_info.get('cluster_id', 'N/A')}{C.r}")
            print(f"{C.b}{C.CLUSTER_PRIMARY}Cluster Creator: {C.r}"
                  f"{C.b}{C.CLUSTER_QUATERNARY}{cluster_info.get('creator_user_name', 'N/A')}{C.r}")

            # Driver details
            print(f"{C.b}{C.CLUSTER_PRIMARY}Driver:{C.r}")
            driver = cluster_info.get('driver', {})
            driver_info = {
                "Private IP": driver.get('private_ip', 'N/A'),
                "Public DNS": driver.get('public_dns', 'N/A'),
                "Start Timestamp": self.convert_timestamp(driver.get('start_timestamp'))
            }
            print(self.format_dict(driver_info, indent=4, key_color=C.CLUSTER_TERTIARY))

            # Executor details
            print(f"{C.b}{C.CLUSTER_PRIMARY}Executors:{C.r}")
            for executor in cluster_info.get('executors', []):
                executor_info = {
                    "Private IP": executor.get('private_ip', 'N/A'),
                    "Public DNS": executor.get('public_dns', 'N/A'),
                    "Start Timestamp": self.convert_timestamp(executor.get('start_timestamp'))
                }
                print(self.format_dict(executor_info, indent=4, key_color=C.CLUSTER_TERTIARY))

            # Other cluster attributes
            print(f"{C.b}{C.CLUSTER_PRIMARY}Other Cluster Details:{C.r}")
            other_info = {
                "Spark Version": cluster_info.get('spark_version', 'N/A'),
                "State": cluster_info.get('state', 'N/A'),
                "Cluster Memory MB": cluster_info.get('cluster_memory_mb', 'N/A'),
                "Cluster Cores": cluster_info.get('cluster_cores', 'N/A'),
                "Autoscale": (
                    f"\n        {C.b}{C.CLUSTER_PRIMARY}Min Workers:{C.r} "
                    f"{cluster_info.get('autoscale', {}).get('min_workers', 'N/A')},"
                    f"\n        {C.b}{C.CLUSTER_PRIMARY}Max Workers:{C.r} "
                    f"{cluster_info.get('autoscale', {}).get('max_workers', 'N/A')},"
                    f"\n        {C.b}{C.CLUSTER_PRIMARY}Target Workers:{C.r} "
                    f"{cluster_info.get('autoscale', {}).get('target_workers', 'N/A')}"
                ),
                "Node Type ID": cluster_info.get('node_type_id', 'N/A'),
                "Driver Node Type ID": cluster_info.get('driver_node_type_id', 'N/A')
            }
            print(self.format_dict(other_info, indent=2, key_color=C.CLUSTER_TERTIARY))
            # Footer
            print(f"{C.CLUSTER_PRIMARY}" + "-" * 60 + f"{C.r}")
        else:
            print(f"{C.b}{C.red}Failed to get cluster info. Status code: {response.status_code}{C.r}")