# src/dqx/utils/config.py

from __future__ import annotations
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession


class ConfigError(RuntimeError):
    pass


class ProjectConfig:
    """
    Loader for YAML of shape:

      version: 1
      variables: { env: dev, ... }
      notebooks: { notebook_1: {...}, notebook_2: {...}, ... }
      run_config_name: { default: {...}, generated_check: {...} }

    Features:
      - Generic {var} expansion for any key in `variables` (unknown braces left intact).
      - Accessors: .notebook(k), .list_notebooks(), .get("path.to.field", default)
      - DBFS path normalization (dbfs:/ → /dbfs/).
      - Variables precedence: caller `variables` > YAML `variables`.
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

        # Variables precedence: caller overrides YAML (no env var reads)
        yaml_vars = raw.get("variables") if isinstance(raw.get("variables"), dict) else {}
        caller_vars = {k: v for k, v in (variables or {}).items() if v is not None}
        merged = {**(yaml_vars or {}), **caller_vars}
        self._vars: Dict[str, str] = {k: str(v) for k, v in merged.items()}

        # Render {var} across the tree (leave unknown tokens intact)
        self._cfg: Dict[str, Any] = self._render_any(raw)

        # Validate notebooks
        nbs = self._cfg.get("notebooks")
        if not isinstance(nbs, dict) or not nbs:
            raise ConfigError("Top-level `notebooks` must be a non-empty mapping like {notebook_1: {...}}.")

        # Build Notebook map
        self._nb_map: Dict[str, NotebookConfig] = {}
        for nk, node in nbs.items():
            if not isinstance(node, dict):
                raise ConfigError(f"`notebooks.{nk}` must be a mapping.")
            name = node.get("name")
            if not name or not isinstance(name, str):
                raise ConfigError(f"`notebooks.{nk}.name` must be a non-empty string.")
            self._nb_map[str(nk)] = NotebookConfig(self, nk, node)

    # ---------- basics ----------
    @property
    def path(self) -> str:
        return str(self._cfg_path)

    @property
    def variables(self) -> Dict[str, str]:
        return dict(self._vars)

    def list_notebooks(self) -> List[str]:
        return sorted(self._nb_map.keys())

    def notebook(self, key_or_index: int | str) -> "NotebookConfig":
        key = f"notebook_{key_or_index}" if isinstance(key_or_index, int) else str(key_or_index)
        nb = self._nb_map.get(key)
        if not nb:
            raise ConfigError(f"Notebook not found: {key}")
        return nb

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self._cfg
        for part in (dotted or "").split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    # ---------- rendering & paths ----------
    def _render_str(self, s: str) -> str:
        if not isinstance(s, str) or "{" not in s:
            return s
        out = s
        for k, v in self._vars.items():
            out = out.replace(f"{{{k}}}", str(v))
        return out

    def _render_any(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._render_any(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._render_any(v) for v in obj]
        return self._render_str(obj) if isinstance(obj, str) else obj

    @staticmethod
    def _normalize_dbfs_like(p: str) -> str:
        if isinstance(p, str) and p.startswith("dbfs:/"):
            return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
        return p


# ---------- Model: Notebook → DataSources/Targets ----------
class NotebookConfig:
    def __init__(self, cfg: ProjectConfig, key: str, node: Dict[str, Any]):
        self._cfg, self._key, self._node = cfg, key, node

    def get(self, key: str, default: Any = None) -> Any:
        return self._node.get(key, default)

    @property
    def name(self) -> str:
        return str(self._node.get("name", ""))

    def data_sources(self) -> "DataSourcesConfig":
        ds = self._node.get("data_sources") or {}
        if not isinstance(ds, dict):
            raise ConfigError(f"`{self._key}.data_sources` must be a mapping (data_source_1, ...).")
        return DataSourcesConfig(self._cfg, ds)

    def targets(self) -> "TargetsConfig":
        t = self._node.get("targets") or {}
        if not isinstance(t, dict) or not t:
            raise ConfigError(f"`{self._key}.targets` must be a non-empty mapping (target_table_1, ...).")
        return TargetsConfig(self._cfg, t)


class DataSourcesConfig:
    def __init__(self, cfg: ProjectConfig, node: Dict[str, Any]):
        self._cfg = cfg
        self._node = node  # mapping: data_source_1 -> spec

    def data_source(self, idx_or_key: int | str) -> "DataSourceSpec":
        key = f"data_source_{idx_or_key}" if isinstance(idx_or_key, int) else str(idx_or_key)
        spec = self._node.get(key)
        if not isinstance(spec, dict):
            raise ConfigError(f"Data source not found: {key}")
        return DataSourceSpec(self._cfg, key, spec)

    def keys(self) -> List[str]:
        return sorted(self._node.keys())


class DataSourceSpec:
    def __init__(self, cfg: ProjectConfig, key: str, node: Dict[str, Any]):
        self._cfg, self._key, self._node = cfg, key, node  # already rendered

    def get(self, key: str, default: Any = None) -> Any:
        return self._node.get(key, default)


class TargetsConfig:
    def __init__(self, cfg: ProjectConfig, node: Dict[str, Any]):
        self._cfg = cfg
        self._node = node  # mapping: target_table_1 -> spec

    def target_table(self, idx_or_key: int | str) -> "TableSpec":
        key = f"target_table_{idx_or_key}" if isinstance(idx_or_key, int) else str(idx_or_key)
        spec = self._node.get(key)
        if not isinstance(spec, dict):
            raise ConfigError(f"Target table not found: {key}")
        return TableSpec(self._cfg, key, spec)

    def keys(self) -> List[str]:
        return sorted(self._node.keys())


class TableSpec:
    def __init__(self, cfg: ProjectConfig, key: str, node: Dict[str, Any]):
        self._cfg, self._key, self._node = cfg, key, node  # already rendered

    def get(self, key: str, default: Any = None) -> Any:
        return self._node.get(key, default)

    def full_table_name(self) -> str:
        cat = (self._node.get("catalog") or "").strip()
        sch = (self._node.get("schema") or "").strip()
        tbl = (self._node.get("table") or "").strip()
        if not (cat and sch and tbl):
            raise ConfigError(
                f"{self._key}: catalog/schema/table are required "
                f"(found: catalog={cat!r}, schema={sch!r}, table={tbl!r})"
            )
        return f"{cat}.{sch}.{tbl}"

    # NEW: always return usable flat tags {key: value}
    def table_tags(self) -> Dict[str, str]:
        raw = self._node.get("table_tags") or {}
        if not isinstance(raw, dict):
            return {}

        # Case 1: nested style: table_tag_1: {key: team, value: data_quality}
        if all(isinstance(v, dict) for v in raw.values()):
            out: Dict[str, str] = {}
            for v in raw.values():
                k = (v.get("key") or "").strip() if isinstance(v, dict) else ""
                val = v.get("value") if isinstance(v, dict) else None
                if k and val is not None:
                    out[k] = str(val)
            return out

        # Case 2: already-flat style: {team: data_quality, project: dqx}
        return {str(k): str(v) for k, v in raw.items() if v is not None}