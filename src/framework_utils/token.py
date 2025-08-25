"""
Module: utils/tokenConfig.py
Description:
    Provides functionality for retrieving Databricks secrets (the API token and instance)
    from a token-management YAML file. It uses DBUtils secrets (or environment variables)
    to fetch tenant_id, client_id, client_secret, and resource, then obtains an OAuth
    access token from Azure AD. It also retrieves the Databricks workspace instance URL
    and exposes convenient properties and unpacking for both values.

Token Management Configuration File (excerpt; only 'prd' shown):
    # File: src/datalake_job_monitor/resources/token-management.yaml
    databricks_token_config:
        prd:
            databricks_token:
                tenant_id:
                    scope: "da-prd-dc01-mgt-kv-dbk"
                    key: "tenant-id"
                client_id:
                    scope: "da-prd-dc01-mgt-kv-dbk"
                    key: "AppReg-da-prd-dc01-analytics-fw-bk-dbk-admin-jobs-worker-ClientID"
                client_secret:
                    scope: "da-prd-dc01-mgt-kv-dbk"
                    key: "AppReg-da-prd-dc01-analytics-fw-bk-dbk-admin-jobs-worker-ClientSecret"
                resource:
                    scope: "da-prd-dc01-mgt-kv-dbk"
                    key: "dbk-resource-id"
            databricks_instance:
                instance_scope: "prd-databricks-api-token-lmg"
                instance_key: "prd_databricks_instance_lmg"

Author: Levi Gagne
Created Date: 2025-04-06
Last Modified: 2025-08-24
"""

import os
import yaml
import requests
from typing import Any, Dict, Tuple
from pyspark.sql import SparkSession

def get_dbutils(spark: SparkSession) -> Any:
    """
    Retrieves the Databricks dbutils object using the provided Spark session.
    
    :param spark: An active SparkSession.
    :return: An instance of DBUtils.
    :raises Exception: If no Spark session is provided.
    :raises ImportError: If DBUtils cannot be imported.
    """
    if spark is None:
        raise Exception("Spark session not provided")
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError as e:
        raise ImportError(
            "DBUtils is not available. Ensure you are running in a Databricks environment."
        ) from e

class TokenConfig:
    """
    Manages retrieval of the Databricks workspace URL and OAuth access token
    using a token-management YAML file and DBUtils or environment variables.
    """
    def __init__(self, config_path: str, env: str, spark: SparkSession = None) -> None:
        """
        Initializes TokenConfig with the token-management YAML file, environment string,
        and optional SparkSession for DBUtils.
        """
        self.spark = spark
        self.dbutils = get_dbutils(spark) if spark is not None else None
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self.load_config()
        self.env = env.lower()
        try:
            self._env_config = self._config["databricks_token_config"][self.env]
        except KeyError:
            raise Exception(
                f"No token configuration found for environment '{self.env}' in {config_path}"
            )

    def load_config(self) -> None:
        """
        Loads the token-management YAML configuration file into self._config.
        """
        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            raise Exception(
                f"Error loading token management YAML from {self.config_path}: {e}"
            )

    def load_secrets(self) -> Tuple[str, str]:
        """
        Retrieves and returns a tuple (full_instance_url, access_token).
        """
        token_cfg = self._env_config.get("databricks_token")
        instance_cfg = self._env_config.get("databricks_instance")
        if not token_cfg or not instance_cfg:
            raise Exception(
                f"Missing token or instance configuration for environment '{self.env}'"
            )

        if self.dbutils is not None:
            tenant_id = self.dbutils.secrets.get(
                scope=token_cfg["tenant_id"]["scope"],
                key=token_cfg["tenant_id"]["key"]
            )
            client_id = self.dbutils.secrets.get(
                scope=token_cfg["client_id"]["scope"],
                key=token_cfg["client_id"]["key"]
            )
            client_secret = self.dbutils.secrets.get(
                scope=token_cfg["client_secret"]["scope"],
                key=token_cfg["client_secret"]["key"]
            )
            resource = self.dbutils.secrets.get(
                scope=token_cfg["resource"]["scope"],
                key=token_cfg["resource"]["key"]
            )
            instance = self.dbutils.secrets.get(
                scope=instance_cfg["instance_scope"],
                key=instance_cfg["instance_key"]
            )
        else:
            tenant_id = os.environ.get(token_cfg["tenant_id"]["key"])
            client_id = os.environ.get(token_cfg["client_id"]["key"])
            client_secret = os.environ.get(token_cfg["client_secret"]["key"])
            resource = os.environ.get(token_cfg["resource"]["key"])
            instance = os.environ.get(instance_cfg["instance_key"])

        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "resource": resource
        }
        response = requests.post(token_url, data=payload)
        if response.status_code != 200:
            raise Exception(
                f"Failed to retrieve token. Status: {response.status_code}, Response: {response.text}"
            )
        token_json = response.json()
        access_token = token_json.get("access_token")
        if not access_token:
            raise Exception("Failed to retrieve access token from OAuth response.")

        # Normalize the instance URL
        instance = instance.rstrip("/")
        if instance.startswith("https://"):
            instance = instance[len("https://"):]
        elif instance.startswith("http://"):
            instance = instance[len("http://"):]
        full_instance_url = f"https://{instance}"

        return full_instance_url, access_token

    def __iter__(self):
        """
        Enables unpacking: instance_url, token = TokenConfig(...)
        """
        return iter(self.load_secrets())

    @property
    def databricks_token(self) -> Dict[str, Any]:
        """
        Returns the OAuth access token in a dict under 'client_secret'.
        """
        _, token = self.load_secrets()
        return {"client_secret": token}

    @property
    def databricks_instance(self) -> str:
        """
        Returns the fully qualified Databricks workspace URL.
        """
        instance, _ = self.load_secrets()
        return instance