from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pyspark.sql import SparkSession


def _normalize_dbfs_like(p: str) -> str:
    # Accept "dbfs:/..." or normal paths; return a local-style path when needed.
    if p.startswith("dbfs:/"):
        return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
    return p


def _is_yaml(fname: str) -> bool:
    low = fname.lower()
    return low.endswith(".yaml") or low.endswith(".yml")


class ProjectConfig:
    """
    Simple, notebook-friendly YAML config loader.

    - Reads the config at the given path.
    - For any relative paths in the config, resolve against the current working directory.
    """

    def __init__(self, config_path: str, spark: Optional[SparkSession] = None):
        cfg_path = Path(_normalize_dbfs_like(config_path))
        if not cfg_path.is_absolute():
            cfg_path = (Path.cwd() / cfg_path).resolve()
        if not cfg_path.exists():
            raise FileNotFoundError(f"Config not found: {cfg_path}")

        self._cfg_path = cfg_path
        self._base_dir = self._cfg_path.parent  # kept for reference
        self.spark = spark

        with open(self._cfg_path, "r") as fh:
            self._cfg: Dict[str, Any] = yaml.safe_load(fh) or {}

    @property
    def raw(self) -> Dict[str, Any]:
        return self._cfg

    def yaml_rules_dir(self) -> str:
        # Resolve relative to the current working directory (not the config file path).
        rules_dir = str(self._cfg.get("dqx_yaml_checks", "")).strip()
        if not rules_dir:
            raise ValueError("Missing 'dqx_yaml_checks' in config.")
        rules_dir = _normalize_dbfs_like(rules_dir)
        p = Path(rules_dir)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        return str(p)

    def checks_config_table(self) -> Tuple[str, str]:
        # Accepts either a string or a mapping {name, primary_key}
        val = self._cfg.get("dqx_checks_config_table_name")
        if isinstance(val, dict):
            nm = val.get("name") or val.get("table") or val.get("table_name")
            if not nm:
                raise ValueError("dqx_checks_config_table_name must include 'name'.")
            pk = val.get("primary_key", "check_id")
            return str(nm), str(pk)
        if not val:
            raise ValueError("Missing 'dqx_checks_config_table_name' in config.")
        return str(val), "check_id"

    def list_rule_files(self, base_dir: str) -> List[str]:
        # Recursively discover *.yaml / *.yml; base_dir resolved relative to CWD if not absolute.
        root = Path(_normalize_dbfs_like(base_dir))
        if not root.is_absolute():
            root = (Path.cwd() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise FileNotFoundError(f"Rules folder not found or not a directory: {root}")

        out: List[str] = []
        for cur, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                if _is_yaml(f):
                    out.append(str(Path(cur) / f))
        return sorted(out)