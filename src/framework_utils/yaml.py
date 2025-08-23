"""
Module: utils/yamlConfig.py
Description:
    Loads and manages the YAML configuration for the job-monitor application,
    providing methods to parse settings, access Databricks dbutils, and construct
    fully qualified target table names based on a provided environment.

Workflow:
    1. get_dbutils(spark):
        - Validates that a SparkSession is provided.
        - Imports and returns a DBUtils instance tied to that SparkSession.
        - Raises an error if dbutils are unavailable or SparkSession is None.
    2. Initialization (__init__):
        - Stores the provided SparkSession and env string (must be non-empty).
        - Retrieves the dbutils instance via get_dbutils().
        - Stores the path to the YAML config file.
        - Calls load_config() to read and parse the YAML into self._config.
        - Calls build_full_table_name() to generate and store the fully
          qualified table name using the provided env.
    3. load_config():
        - Opens the YAML file at config_path.
        - Parses its contents with yaml.safe_load into self._config.
        - Raises an exception if file access or parsing fails.
    4. build_full_table_name():
        - Extracts 'catalog', 'schema', and 'table_name' from
          self._config['app_config']['target_table'].
        - Uses the self.env attribute (no fallback allowed).
        - Concatenates catalog + env to form the full catalog name.
        - Returns "<full_catalog>.<schema>.<table_name>".
    5. Properties:
        - config: Returns the raw parsed YAML configuration dictionary.
        - full_table_name: Returns the computed fully qualified table name.
"""

import yaml
from typing import Any, Dict

from pyspark.sql import SparkSession


def get_dbutils(spark: SparkSession) -> Any:
    """
    Retrieves the Databricks dbutils object using the provided Spark session.

    :param spark: An active SparkSession.
    :return: An instance of DBUtils.
    :raises ValueError: If no Spark session is provided.
    :raises ImportError: If DBUtils cannot be imported.
    """
    if spark is None:
        raise ValueError("Spark session not provided")
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except (ImportError, TypeError) as e:
        raise ImportError(
            "DBUtils is not available. Ensure you are running in a Databricks environment."
        ) from e


class YamlConfig:
    """
    Loads and provides access to YAML configuration settings for the job-monitor
    application, including constructing fully qualified table names based on
    a provided environment and YAML values.
    """
    def __init__(
        self,
        config_path: str,
        env: str,
        spark: SparkSession = None
    ):
        """
        Initializes YamlConfig.

        :param config_path: Path to the YAML configuration file.
        :param env: Environment string (e.g., "dev", "prd"). Must be non-empty.
        :param spark: SparkSession for dbutils operations.
        :raises ValueError: If env is empty or None.
        """
        if not env:
            raise ValueError("You must provide a non-empty 'env' to YamlConfig")
        self.config_path = config_path
        self.env = env
        self.spark = spark
        self.dbutils = get_dbutils(spark)
        self._config: Dict[str, Any] = {}
        self.load_config()
        self._full_table_name = self.build_full_table_name()

    def load_config(self) -> None:
        """
        Loads the YAML configuration file from the provided path into self._config.

        :raises ValueError: If file cannot be opened or parsing fails.
        """
        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise ValueError(
                f"Error loading YAML configuration from {self.config_path}: {e}"
            )

    def build_full_table_name(self) -> str:
        """
        Constructs the fully qualified table name from the YAML configuration and
        the specified environment.

        :return: Fully qualified table name, e.g., "dq_dev.monitoring.job_run_audit".
        """
        target = self._config.get("app_config", {}).get("target_table", {})
        catalog = target.get("catalog", "").strip()
        schema = target.get("schema", "").strip()
        table = target.get("table_name", "").strip()

        full_catalog = f"{catalog}{self.env}"
        return f"{full_catalog}.{schema}.{table}"

    @property
    def config(self) -> Dict[str, Any]:
        """
        Returns the parsed YAML configuration dictionary.
        """
        return self._config

    @property
    def full_table_name(self) -> str:
        """
        Returns the fully qualified target table name computed during initialization.
        """
        return self._full_table_name