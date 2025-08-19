# src/dqx/utils/config.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pyspark.sql import SparkSession


class ProjectConfig:
    """
    Minimal, notebook-friendly config loader.

    - Reads a single YAML config file.
    - Resolves any relative paths against the current working directory.
    - Provides helpers to list YAML rule files and to load a YAML rules file.
    """

    def __init__(self, config_path: str, spark: Optional[SparkSession] = None):
        p = Path(self._normalize_dbfs_like(config_path))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {p}")
        self._cfg_path = p
        self._base_dir = p.parent  # not used externally but handy to keep
        self.spark = spark

        with open(p, "r") as fh:
            self._cfg: Dict[str, Any] = yaml.safe_load(fh) or {}

    # -------- public accessors --------
    @property
    def raw(self) -> Dict[str, Any]:
        return self._cfg

    def yaml_rules_dir(self) -> str:
        """Return the absolute path to the folder containing YAML rules."""
        rules_dir = str(self._cfg.get("dqx_yaml_checks", "")).strip()
        if not rules_dir:
            raise ValueError("Missing 'dqx_yaml_checks' in config.")
        p = Path(self._normalize_dbfs_like(rules_dir))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        return str(p)

    def checks_config_table(self) -> Tuple[str, str]:
        """
        Return (fully_qualified_table_name, primary_key).
        Accepts either a string or a mapping {name, primary_key} in the config.
        """
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
        """
        Recursively enumerate *.yaml / *.yml under base_dir (resolved vs CWD).
        """
        root = Path(self._normalize_dbfs_like(base_dir))
        if not root.is_absolute():
            root = (Path.cwd() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise FileNotFoundError(f"Rules folder not found or not a directory: {root}")

        out: List[str] = []
        for cur, dirs, files in os.walk(root):
            # skip dot-directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                if self._is_yaml(f):
                    out.append(str(Path(cur) / f))
        return sorted(out)

    # -------- static helpers kept inside the class --------
    @staticmethod
    def _normalize_dbfs_like(p: str) -> str:
        if p.startswith("dbfs:/"):
            return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
        return p

    @staticmethod
    def _is_yaml(fname: str) -> bool:
        low = (fname or "").lower()
        return low.endswith(".yaml") or low.endswith(".yml")

    @staticmethod
    def load_yaml_rules(path: str) -> List[dict]:
        """
        Read one YAML file that may contain multiple documents.
        Flattens dict docs and lists of dicts.
        """
        p = Path(ProjectConfig._normalize_dbfs_like(path))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if not p.exists():
            raise FileNotFoundError(f"YAML file not found: {p}")

        with open(p, "r") as fh:
            docs = list(yaml.safe_load_all(fh)) or []

        out: List[dict] = []
        for d in docs:
            if not d:
                continue
            if isinstance(d, dict):
                out.append(d)
            elif isinstance(d, list):
                out.extend([x for x in d if isinstance(x, dict)])
        return out


# needed for list_rule_files
import os  # keep at bottom to avoid shadowing by staticmethod names