# src/dqx/utils/config.py

from __future__ import annotations

import os
from pathlib import Path
from string import Formatter
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pyspark.sql import SparkSession


class _SafeMap(dict):
    """Return {key} literally when missing during format_map."""
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class ProjectConfig:
    """
    Minimal, notebook-friendly config loader.

    - Reads a single YAML config file.
    - Resolves any relative paths against CWD.
    - Handles simple {var} templating from dqx_config.variables or passed-in variables.
    - Exposes helpers for rules discovery, target tables, and table documentation.
    """

    def __init__(
        self,
        config_path: str,
        spark: Optional[SparkSession] = None,
        variables: Optional[Dict[str, str]] = None,
    ):
        p = Path(self._normalize_dbfs_like(config_path))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {p}")
        self._cfg_path = p
        self._base_dir = p.parent
        self.spark = spark

        with open(p, "r") as fh:
            self._cfg: Dict[str, Any] = yaml.safe_load(fh) or {}

        # variables for {env} etc.
        vars_from_yaml = (
            (self._cfg.get("dqx_config") or {}).get("variables") or {}
        )
        self._vars: Dict[str, str] = {
            "env": os.environ.get("DQX_ENV", "dev"),
            **vars_from_yaml,
            **(variables or {}),
        }

    # -------- public accessors --------
    @property
    def raw(self) -> Dict[str, Any]:
        return self._cfg

    # timezones / flags
    def local_timezone(self) -> str:
        return ((self._cfg.get("dqx_config") or {}).get("local_timezone")) or "UTC"

    def processing_timezone(self) -> str:
        return ((self._cfg.get("dqx_config") or {}).get("processing_timezone")) or "UTC"

    def apply_table_metadata_flag(self) -> bool:
        return bool((self._cfg.get("dqx_config") or {}).get("apply_table_metadata", False))

    # rules discovery
    def yaml_rules_dir(self) -> str:
        """Return absolute path to folder containing YAML rules (back-compat name)."""
        node = ((self._cfg.get("dqx_load_checks") or {}).get("checks_config_source") or {})
        rules_dir = str(node.get("source_path") or self._cfg.get("dqx_yaml_checks", "")).strip()
        if not rules_dir:
            raise ValueError("Missing rules directory. Set dqx_load_checks.checks_config_source.source_path.")
        p = Path(self._normalize_dbfs_like(rules_dir))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        return str(p)

    # validation controls
    def allowed_criticality(self) -> List[str]:
        node = ((self._cfg.get("dqx_load_checks") or {}).get("checks_config_source") or {})
        vals = node.get("allowed_criticality") or ["error", "warn", "warning"]
        return [str(v) for v in vals]

    def required_fields(self) -> List[str]:
        node = ((self._cfg.get("dqx_load_checks") or {}).get("checks_config_source") or {})
        vals = node.get("required_fields") or ["table_name", "name", "criticality", "run_config_name", "check"]
        return [str(v) for v in vals]

    # target table names
    def checks_config_table(self) -> Tuple[str, str]:
        """Return (fully_qualified_table_name, primary_key)."""
        node = ((self._cfg.get("dqx_load_checks") or {}).get("checks_config_table") or {})
        name = node.get("target_table") or (self._cfg.get("dqx_checks_config_table_name") if isinstance(self._cfg.get("dqx_checks_config_table_name"), str) else None)
        if not name:
            raise ValueError("checks_config_table.target_table is required.")
        pk = node.get("primary_key") or "check_id"
        return self._render(name), str(pk)

    def checks_log_table(self) -> Tuple[str, str]:
        node = ((self._cfg.get("dqx_run_checks") or {}).get("checks_log_table") or {})
        name = node.get("target_table") or (self._cfg.get("dqx_checks_log_table_name") if isinstance(self._cfg.get("dqx_checks_log_table_name"), str) else None)
        if not name:
            raise ValueError("dqx_run_checks.checks_log_table.target_table is required.")
        pk = node.get("primary_key") or "log_id"
        return self._render(name), str(pk)

    # documentation templates (table + column comments)
    def table_doc(self, table_section: str) -> Optional[Dict[str, Any]]:
        """
        table_section: 'checks_config_table' | 'checks_log_table' | etc.
        Returns dict with keys: table (placeholder), table_comment, columns{name->comment}
        """
        if table_section == "checks_config_table":
            node = ((self._cfg.get("dqx_load_checks") or {}).get("checks_config_table") or {})
        else:
            node = ((self._cfg.get("dqx_run_checks") or {}).get(table_section) or {})

        if not node:
            return None

        tbl_comment = node.get("table_comment", "") or ""
        cols_node = node.get("columns") or {}
        # normalize: support either {name:{...,comment}} or {name: "comment"} styles
        cols_comments: Dict[str, str] = {}
        for k, v in cols_node.items():
            if isinstance(v, dict):
                comment = v.get("comment") or v.get("column_description") or ""
            else:
                comment = str(v or "")
            cols_comments[str(k)] = str(comment)

        return {
            "table": "<override at create time>",
            "table_comment": str(tbl_comment),
            "columns": cols_comments,
        }

    # list rule files recursive
    def list_rule_files(self, base_dir: str) -> List[str]:
        root = Path(self._normalize_dbfs_like(base_dir))
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
                if self._is_yaml(f):
                    out.append(str(Path(cur) / f))
        return sorted(out)

    # -------- static helpers --------
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

    # formatting for {variables}
    def _render(self, s: Any) -> str:
        if not isinstance(s, str):
            return str(s)
        # only format when there is at least one {...}
        if any(t[1] is not None for t in Formatter().parse(s)):
            return s.format_map(_SafeMap(self._vars))
        return s


# needed for list_rule_files
import os  # keep at bottom