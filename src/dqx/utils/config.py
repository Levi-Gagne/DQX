# src/dqx/utils/config.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

import yaml
from pyspark.sql import SparkSession


class ConfigError(RuntimeError):
    pass


# ----------------------------
# Core: ProjectConfig
# ----------------------------
class ProjectConfig:
    """
    Config loader for your YAML shape:

      project_config: {..., variables: {env: dev}}
      notebooks:
        notebook_1:
          name: 01_load_dqx_checks
          data_source: {...}
          targets:
            target_table_1: {catalog: "dq_{env}", schema: "dqx", table: "checks_config", ...}
        notebook_2:
          ...

    Features:
      - Only substitutes {env}. All other braces are left as-is.
      - Accepts `notebook(1)` (-> "notebook_1") or `notebook("notebook_1")`.
      - Object model: NotebookConfig -> TargetsConfig -> TableSpec
      - Generic dotted get: cfg.get("project_config.local_timezone", "UTC")
      - Helpers: .path, .list_notebooks()
    """

    def __init__(
        self,
        config_path: str,
        *,
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
            raw = yaml.safe_load(fh) or {}

        # Resolve variables (only {env})
        globals_node = (raw.get("project_config") or {})
        vars_from_yaml = globals_node.get("variables") or {}
        self._vars: Dict[str, str] = {
            "env": os.environ.get("DQX_ENV", ""),  # env var may be empty
            **vars_from_yaml,
            **(variables or {}),
        }

        # Render {env} across the whole tree, leaving all other braces intact.
        self._cfg: Dict[str, Any] = self._render_any(raw)

        # Validate expected top-level keys
        if "notebooks" not in self._cfg or not isinstance(self._cfg["notebooks"], dict):
            raise ConfigError("Top-level `notebooks` must be a mapping like {notebook_1: {...}, ...}.")

        # Build notebook map
        self._nb_map: Dict[str, NotebookConfig] = {}
        for nk, node in self._cfg["notebooks"].items():
            if not isinstance(node, dict):
                raise ConfigError(f"`notebooks.{nk}` must be a mapping.")
            name = node.get("name")
            if not name or not isinstance(name, str):
                raise ConfigError(f"`notebooks.{nk}.name` must be a non-empty string.")
            self._nb_map[str(nk)] = NotebookConfig(self, nk, node)

    # ---------- basic ----------
    @property
    def path(self) -> str:
        return str(self._cfg_path)

    def list_notebooks(self) -> List[str]:
        return sorted(self._nb_map.keys())

    def notebook(self, key_or_index: int | str) -> "NotebookConfig":
        if isinstance(key_or_index, int):
            key = f"notebook_{key_or_index}"
        else:
            key = str(key_or_index)
        nb = self._nb_map.get(key)
        if not nb:
            raise ConfigError(f"Notebook not found: {key}")
        return nb

    # ---------- generic dotted get ----------
    def get(self, dotted: str, default: Any = None) -> Any:
        """
        Example: cfg.get("project_config.local_timezone", "UTC")
        """
        node: Any = self._cfg
        for part in (dotted or "").split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    # ---------- files / discovery helpers ----------
    def list_rule_files(self, base_dir: str) -> List[str]:
        """
        Recursively list YAML files under base_dir (supports dbfs:/).
        """
        root = Path(self._normalize_dbfs_like(base_dir))
        if not root.is_absolute():
            root = (Path.cwd() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise FileNotFoundError(f"Rules folder not found or not a directory: {root}")

        out: List[str] = []
        for cur, dirs, files in os.walk(root):
            # prune dot-dirs
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                if self._is_yaml(f):
                    out.append(str(Path(cur) / f))
        return sorted(out)

    # ---------- internals ----------
    def _render(self, s: Any) -> str:
        # Only expand {env}; keep all other braces literally.
        if not isinstance(s, str):
            return str(s)
        env_val = "" if self._vars is None else str(self._vars.get("env", ""))
        return s.replace("{env}", env_val)

    def _render_any(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._render_any(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._render_any(v) for v in obj]
        return self._render(obj)

    @staticmethod
    def _normalize_dbfs_like(p: str) -> str:
        if isinstance(p, str) and p.startswith("dbfs:/"):
            return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
        return p

    @staticmethod
    def _is_yaml(fname: str) -> bool:
        low = (fname or "").lower()
        return low.endswith(".yaml") or low.endswith(".yml")


# ----------------------------
# Model: Notebook -> Targets -> TableSpec
# ----------------------------
class NotebookConfig:
    def __init__(self, cfg: ProjectConfig, key: str, node: Dict[str, Any]):
        self._cfg = cfg
        self._key = key
        self._node = node

    def get(self, key: str, default: Any = None) -> Any:
        """
        Shallow get of fields under this notebook node (already rendered).
        Example: nb.get("data_source", {})
        """
        v = self._node.get(key, default)
        return v

    @property
    def name(self) -> str:
        return str(self._node.get("name", ""))

    def targets(self) -> "TargetsConfig":
        t = self._node.get("targets")
        if not isinstance(t, dict) or not t:
            raise ConfigError(f"`{self._key}.targets` must be a non-empty mapping (target_table_1, ...).")
        return TargetsConfig(self._cfg, t)


class TargetsConfig:
    def __init__(self, cfg: ProjectConfig, node: Dict[str, Any]):
        self._cfg = cfg
        self._node = node  # mapping: target_table_1 -> spec

    def target_table(self, idx_or_key: int | str) -> "TableSpec":
        if isinstance(idx_or_key, int):
            key = f"target_table_{idx_or_key}"
        else:
            key = str(idx_or_key)
        spec = self._node.get(key)
        if not isinstance(spec, dict):
            raise ConfigError(f"Target table not found: {key}")
        return TableSpec(self._cfg, key, spec)

    def keys(self) -> List[str]:
        return sorted(self._node.keys())


class TableSpec:
    def __init__(self, cfg: ProjectConfig, key: str, node: Dict[str, Any]):
        self._cfg = cfg
        self._key = key
        self._node = node  # already rendered

    def get(self, key: str, default: Any = None) -> Any:
        return self._node.get(key, default)

    def full_table_name(self) -> str:
        cat = (self._node.get("catalog") or "").strip()
        sch = (self._node.get("schema") or "").strip()
        tbl = (self._node.get("table") or "").strip()
        if not (cat and sch and tbl):
            raise ConfigError(
                f"{self._key}: catalog/schema/table are required to build full table name (found: "
                f"catalog={cat!r}, schema={sch!r}, table={tbl!r})"
            )
        return f"{cat}.{sch}.{tbl}"