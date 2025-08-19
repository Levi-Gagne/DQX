# utils/config.py
from __future__ import annotations

import os
import yaml
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import SparkSession  # type: ignore

from utils.path import resolve_resource, normalize_path


class ProjectConfig:
    """
    Loader for project-level YAML config with a couple of helpers for paths/values.
    """

    def __init__(
        self,
        config_path: str,
        *,
        spark: Optional[SparkSession] = None,
        env: Optional[str] = None,
        start_file: Optional[str] = None,
    ):
        self.spark = spark
        self.env = (env or "").strip().lower()
        self.start_file = start_file
        self._dbutils = self._init_dbutils(spark)
        self._path_in = config_path
        self._cfg: Dict[str, Any] = {}
        self._load()

    # attempt both dbutils entry points
    def _init_dbutils(self, spark: Optional[SparkSession]):
        try:
            # available on DBR 13+ notebooks/jobs
            from databricks.sdk.runtime import dbutils  # type: ignore
            return dbutils
        except Exception:
            pass
        if spark is not None:
            try:
                from pyspark.dbutils import DBUtils  # type: ignore
                return DBUtils(spark)
            except Exception:
                pass
        return None

    # read the YAML config file (supports repo-relative and dbfs:/)
    def _load(self) -> None:
        p = resolve_resource(self._path_in, start_file=self.start_file or __file__)
        with open(p, "r") as fh:
            self._cfg = yaml.safe_load(fh) or {}

    # raw dict when needed
    @property
    def config(self) -> Dict[str, Any]:
        return self._cfg

    # dqx rules folder (config key: dqx_yaml_checks)
    def yaml_rules_dir(self) -> str:
        raw = self._cfg.get("dqx_yaml_checks") or ""
        return normalize_path(raw, start_file=self.start_file or __file__)

    # checks config table name + PK (accepts string or mapping with {name, primary_key})
    def checks_config_table(self) -> Tuple[str, str]:
        val = self._cfg.get("dqx_checks_config_table_name")
        if isinstance(val, dict):
            name = val.get("name") or val.get("table") or val.get("table_name")
            if not name:
                raise ValueError("dqx_checks_config_table_name must include 'name' when provided as a mapping.")
            pk = val.get("primary_key", "check_id")
            return str(name), str(pk)
        if isinstance(val, str) and val.strip():
            return val.strip(), "check_id"
        raise ValueError("dqx_checks_config_table_name not set")

    # checks log table (dataset for row-level issues)
    def checks_log_table(self) -> str:
        val = self._cfg.get("dqx_checks_log_table_name")
        if not val:
            raise ValueError("dqx_checks_log_table_name not set")
        return str(val)

    # run configs block (used by runners)
    def run_configs(self) -> Dict[str, Any]:
        return dict(self._cfg.get("run_config_name") or {})

    # optional: secrets via dbutils
    def get_secret(self, scope: str, key: str) -> str:
        if not self._dbutils:
            raise RuntimeError("dbutils not available")
        return self._dbutils.secrets.get(scope=scope, key=key)

    # enumerate YAML rule files recursively (hidden files ignored)
    def list_rule_files(self, base_dir: Optional[str] = None) -> List[str]:
        from os import walk
        from os.path import join
        root = normalize_path(base_dir or self.yaml_rules_dir(), start_file=self.start_file or __file__)
        out: List[str] = []
        for r, dirs, files in walk(root):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                fl = f.lower()
                if fl.endswith(".yaml") or fl.endswith(".yml"):
                    out.append(join(r, f))
        return sorted(out)

    # multi-doc YAML parse → list of dicts
    def read_yaml_rules_file(self, path: str) -> List[dict]:
        p = normalize_path(path, start_file=self.start_file or __file__)
        with open(p, "r") as fh:
            docs = list(yaml.safe_load_all(fh))
        out: List[dict] = []
        for d in docs:
            if d is None:
                continue
            if isinstance(d, dict):
                out.append(d)
            elif isinstance(d, list):
                out.extend([x for x in d if isinstance(x, dict)])
        return out