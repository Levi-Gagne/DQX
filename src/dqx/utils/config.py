from __future__ import annotations

import os
from pathlib import Path
from string import Formatter
from typing import Any, Dict, List, Optional, Tuple, Callable

import yaml
from pyspark.sql import SparkSession


class ConfigError(RuntimeError):
    pass


class _SafeMap(dict):
    def __missing__(self, key: str) -> str:
        # leave unresolved placeholders intact: "{key}"
        return "{" + key + "}"


class ProjectConfig:
    """
    Strict, generic config loader.

    - Exposes generic getters for:
        * global values: project_config.*
        * notebooks: explicit name; each has optional data_source and a list of target_tables
        * arbitrary nested keys via get_value(path, required=True/False)
    - No baked-in defaults. If required=True and the key is missing, raises ConfigError.
    - String templates are rendered with {variables} found in project_config.variables and env (DQX_ENV).
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
            self._cfg: Dict[str, Any] = yaml.safe_load(fh) or {}

        globals_node = self._cfg.get("project_config") or {}
        # LEGACY: if you do NOT want to read legacy location at all, delete the next line.
        if not globals_node:  # LEGACY support
            globals_node = self._cfg.get("dqx_config") or {}

        vars_from_yaml = globals_node.get("variables") or {}
        self._vars: Dict[str, str] = {
            "env": os.environ.get("DQX_ENV", ""),  # env var may be empty; no default semantics
            **vars_from_yaml,
            **(variables or {}),
        }

        nbs = self._cfg.get("notebooks")
        if nbs is None or not isinstance(nbs, list):
            raise ConfigError("Top-level `notebooks` must be a list with at least one notebook.")
        self._nb_by_name: Dict[str, Dict[str, Any]] = {}
        for nb in nbs:
            if not isinstance(nb, dict) or not nb.get("name"):
                raise ConfigError("Each item in `notebooks` must be a dict with a non-empty `name`.")
            self._nb_by_name[str(nb["name"])] = nb

    # ---------- core ----------
    @property
    def raw(self) -> Dict[str, Any]:
        return self._cfg

    @property
    def path(self) -> str:
        return str(self._cfg_path)

    def list_notebooks(self) -> List[str]:
        return sorted(self._nb_by_name.keys())

    def notebook(self, name: str) -> Dict[str, Any]:
        node = self._nb_by_name.get(name)
        if not node:
            raise ConfigError(f"Notebook not found: {name!r}")
        return node

    # ---------- global values ----------
    def global_value(
        self,
        key: str,
        *,
        required: bool = False,
        cast: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        node = self._cfg.get("project_config") or {}
        # LEGACY: if you do NOT want to read legacy location at all, delete the next line.
        if key not in node:  # LEGACY support
            node = (self._cfg.get("project_config") or {}) | (self._cfg.get("dqx_config") or {})

        val = node.get(key, None)
        if val is None and required:
            raise ConfigError(f"Missing required global key: project_config.{key}")
        if isinstance(val, str):
            val = self._render(val)
        if cast and val is not None:
            try:
                val = cast(val)
            except Exception as e:
                raise ConfigError(f"Invalid value for project_config.{key}: {e}")
        return val

    # Convenience wrappers (strict — all required)
    def local_timezone(self) -> str:
        return str(self.global_value("local_timezone", required=True))

    def processing_timezone(self) -> str:
        return str(self.global_value("processing_timezone", required=True))

    def apply_table_metadata_flag(self) -> bool:
        v = self.global_value("apply_table_metadata", required=True)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            low = v.strip().lower()
            if low in {"true", "1", "yes", "y"}:
                return True
            if low in {"false", "0", "no", "n"}:
                return False
        raise ConfigError("project_config.apply_table_metadata must be boolean or boolean-like string.")

    def created_by(self) -> str:
        return str(self.global_value("created_by", required=True))

    def batch_dedupe_mode(self) -> str:
        return str(self.global_value("batch_dedupe_mode", required=True))

    # ---------- data_source (generic) ----------
    def data_source(self, notebook_name: str) -> Dict[str, Any]:
        nb = self.notebook(notebook_name)
        ds = nb.get("data_source")
        if ds is None:
            return {}
        if not isinstance(ds, dict):
            raise ConfigError(f"{notebook_name}: data_source must be a mapping.")
        return self._render_any(ds)

    def data_source_value(
        self,
        notebook_name: str,
        key: str,
        *,
        required: bool = False,
        cast: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        ds = self.data_source(notebook_name)
        val = ds.get(key, None)
        if val is None and required:
            raise ConfigError(f"{notebook_name}: missing required data_source.{key}")
        if cast and val is not None:
            try:
                val = cast(val)
            except Exception as e:
                raise ConfigError(f"{notebook_name}: invalid data_source.{key}: {e}")
        return val

    # ---------- target tables (generic) ----------
    def target_tables(self, notebook_name: str) -> List[Dict[str, Any]]:
        nb = self.notebook(notebook_name)
        arr = nb.get("target_tables")
        if not isinstance(arr, list):
            raise ConfigError(f"{notebook_name}: target_tables must be a non-empty list.")
        out: List[Dict[str, Any]] = []
        for t in arr:
            if not isinstance(t, dict):
                raise ConfigError(f"{notebook_name}: each target_tables entry must be a mapping.")
            out.append(self._render_any(t))
        return out

    def table(
        self,
        notebook_name: str,
        table_name: str,
        *,
        required: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Find a table by its (possibly templated) name under a notebook.
        """
        rendered = self._render(table_name)
        for t in self.target_tables(notebook_name):
            if self._render(t.get("name", "")) == rendered:
                return t
        if required:
            raise ConfigError(f"{notebook_name}: table not found: {rendered}")
        return None

    # ---------- files / YAML loading ----------
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

    # ---------- helpers ----------
    def _globals_node(self) -> Dict[str, Any]:
        return self._cfg.get("project_config") or {}

    @staticmethod
    def _normalize_dbfs_like(p: str) -> str:
        if isinstance(p, str) and p.startswith("dbfs:/"):
            return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
        return p

    @staticmethod
    def _is_yaml(fname: str) -> bool:
        low = (fname or "").lower()
        return low.endswith(".yaml") or low.endswith(".yml")

    def _render(self, s: Any) -> str:
        if not isinstance(s, str):
            return str(s)
        if any(t[1] is not None for t in Formatter().parse(s)):
            return s.format_map(_SafeMap(self._vars))
        return s

    def _render_any(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._render_any(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._render_any(v) for v in obj]
        return self._render(obj)