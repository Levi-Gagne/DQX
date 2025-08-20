# src/dqx/config_REFACTOR.py
from __future__ import annotations

import os
from pathlib import Path
from string import Formatter
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import yaml

Json = Dict[str, Any]


class ConfigError(RuntimeError):
    pass


# ---------- tiny utils ----------
class _SafeMap(dict):
    """String .format_map helper that leaves {missing} placeholders intact."""
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _is_yaml(fname: str) -> bool:
    low = (fname or "").lower()
    return low.endswith(".yaml") or low.endswith(".yml")


def _normalize_dbfs_like(p: str) -> str:
    if isinstance(p, str) and p.startswith("dbfs:/"):
        return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
    return p


def _render_string(s: Any, vars_map: Dict[str, str]) -> str:
    if not isinstance(s, str):
        return str(s)
    if any(t[1] is not None for t in Formatter().parse(s)):
        return s.format_map(_SafeMap(vars_map))
    return s


def _render_any(obj: Any, vars_map: Dict[str, str]) -> Any:
    if isinstance(obj, dict):
        return {k: _render_any(v, vars_map) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_render_any(v, vars_map) for v in obj]
    return _render_string(obj, vars_map)


# ---------- positional key helpers ----------
def notebook_key(i: int) -> str:
    return f"notebook_{int(i)}"


def target_key(i: int) -> str:
    return f"target_table_{int(i)}"


def column_key(i: int) -> str:
    return f"column_{int(i)}"


def field_key(i: int) -> str:
    return f"field_{int(i)}"


def table_tag_key(i: int) -> str:
    return f"table_tag_{int(i)}"


def column_tag_key(i: int) -> str:
    return f"column_tag_{int(i)}"


# ---------- chainable view ----------
class Node:
    """
    A lightweight view over a sub-tree of the config dict.
    Provides safe get() and positional conveniences.
    """
    def __init__(self, cfg: "ProjectConfig", data: Json, path: str):
        self._cfg = cfg
        self._data = data
        self._path = path  # dotted path for error messages

    def exists(self) -> bool:
        return self._data is not None

    def raw(self) -> Json:
        return self._data

    def get(self, path: str = "", default: Any = None, *, cast: Optional[Callable[[Any], Any]] = None) -> Any:
        """
        Safe getter under this node using dotted 'a.b.c' paths.
        Empty path returns the node itself.
        """
        if not path:
            return self._data
        cur: Any = self._data
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        val = cur
        if cast and val is not None:
            try:
                val = cast(val)
            except Exception as e:
                raise ConfigError(f"Invalid cast for {self._path}.{path}: {e}")
        return val

    # --- navigation helpers (positional) ---
    def targets(self) -> "Node":
        t = self.get("targets", {})
        return Node(self._cfg, t, f"{self._path}.targets")

    def target_table(self, i: int) -> "Node":
        t = self.targets().get(target_key(i), {})
        return Node(self._cfg, t, f"{self._path}.targets.{target_key(i)}")

    def columns(self) -> "Node":
        c = self.get("columns", {})
        return Node(self._cfg, c, f"{self._path}.columns")

    def column(self, i: int) -> "Node":
        c = self.columns().get(column_key(i), {})
        return Node(self._cfg, c, f"{self._path}.columns.{column_key(i)}")

    def fields(self) -> "Node":
        f = self.get("fields", {})
        return Node(self._cfg, f, f"{self._path}.fields")

    def field(self, i: int) -> "Node":
        f = self.fields().get(field_key(i), {})
        return Node(self._cfg, f, f"{self._path}.fields.{field_key(i)}")

    # --- convenience for FQN on any node that has catalog/schema/table ---
    def full_table_name(self) -> str:
        cat = self.get("catalog")
        sch = self.get("schema")
        tbl = self.get("table")
        if not all([cat, sch, tbl]):
            raise ConfigError(f"{self._path}: catalog/schema/table must be set to build FQN.")
        return f"{cat}.{sch}.{tbl}"


class ProjectConfig:
    """
    Minimal, strict loader for the *positional* DQX YAML schema.

    Key features:
      - Variables: project_config.variables + env DQX_ENV are available to template strings.
      - Safe getters:
          * cfg.get("project_config.local_timezone")
          * cfg.notebook(1).target_table(2).column(3).get("name")
      - Positional helpers: notebook(i), target_table(i), column(i), field(i)
      - FQN helpers: cfg.table_fqn(1, 2) or cfg.notebook(1).target_table(2).full_table_name()
      - Rendered snapshot: cfg.rendered returns the fully interpolated dict
    """

    def __init__(
        self,
        config_path: str,
        *,
        variables: Optional[Dict[str, str]] = None,
    ):
        p = Path(_normalize_dbfs_like(config_path))
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {p}")
        self._cfg_path = p

        with open(p, "r") as fh:
            raw: Json = yaml.safe_load(fh) or {}

        proj = raw.get("project_config") or {}
        vars_from_yaml = (proj.get("variables") or {}).copy()
        # Compose variable map with env fallback (DQX_ENV) and user overrides
        self._vars: Dict[str, str] = {
            "env": os.environ.get("DQX_ENV", vars_from_yaml.get("env", "")),
            **vars_from_yaml,
            **(variables or {}),
        }

        # Validate top-level shape just enough to be useful (no over-policing)
        nbs = raw.get("notebooks")
        if not isinstance(nbs, dict):
            raise ConfigError("`notebooks` must be a mapping of {notebook_1: {...}, ...}.")

        # Keep both raw + rendered (interpolated) snapshots
        self._raw: Json = raw
        self._rendered: Json = _render_any(raw, self._vars)

    # ---------- core ----------
    @property
    def path(self) -> str:
        return str(self._cfg_path)

    @property
    def raw(self) -> Json:
        """Unrendered YAML as loaded (placeholders intact)."""
        return self._raw

    @property
    def rendered(self) -> Json:
        """Rendered YAML snapshot (placeholders resolved with variables)."""
        return self._rendered

    def variables(self) -> Dict[str, str]:
        return dict(self._vars)

    # ---------- generic safe getter ----------
    def get(self, path: str, default: Any = None, *, cast: Optional[Callable[[Any], Any]] = None) -> Any:
        """
        Safe getter using dotted paths, e.g.:
          cfg.get("project_config.local_timezone")
          cfg.get("notebooks.notebook_1.targets.target_table_2.columns.column_3.name")
        Returns `default` if any segment is missing.
        """
        cur: Any = self._rendered
        if not path:
            return cur
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        val = cur
        if cast and val is not None:
            try:
                val = cast(val)
            except Exception as e:
                raise ConfigError(f"Invalid cast for {path}: {e}")
        return val

    # ---------- positional navigation (chainable) ----------
    def notebooks(self) -> Node:
        n = self.get("notebooks", {})
        return Node(self, n, "notebooks")

    def notebook(self, i: int) -> Node:
        n = self.notebooks().get(notebook_key(i), {})
        return Node(self, n, f"notebooks.{notebook_key(i)}")

    # ---------- table helpers ----------
    def table_node(self, notebook_idx: int, target_idx: int) -> Node:
        return self.notebook(notebook_idx).targets().target_table(target_idx)

    def table_fqn(self, notebook_idx: int, target_idx: int) -> str:
        """
        Build <catalog>.<schema>.<table> with variables already rendered.
        """
        return self.table_node(notebook_idx, target_idx).full_table_name()

    # ---------- convenience column helpers ----------
    def column_node(self, notebook_idx: int, target_idx: int, column_idx: int) -> Node:
        return self.table_node(notebook_idx, target_idx).column(column_idx)

    def column_value(
        self,
        notebook_idx: int,
        target_idx: int,
        column_idx: int,
        key: str,
        default: Any = None,
        *,
        cast: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        return self.column_node(notebook_idx, target_idx, column_idx).get(key, default, cast=cast)

    # ---------- path-based value with positional sugar ----------
    def get_positional(
        self,
        *,
        notebook: Optional[int] = None,
        target_table: Optional[int] = None,
        column: Optional[int] = None,
        field: Optional[int] = None,
        tail_path: str = "",
        default: Any = None,
        cast: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        """
        Build a dotted path for you using positional indices, e.g.:
          get_positional(notebook=1, target_table=2, column=3, tail_path="name")
        """
        parts: List[str] = []
        if notebook is not None:
            parts += ["notebooks", notebook_key(notebook)]
        if target_table is not None:
            parts += ["targets", target_key(target_table)]
        if column is not None:
            parts += ["columns", column_key(column)]
        if field is not None:
            parts += ["fields", field_key(field)]
        if tail_path:
            parts += tail_path.split(".")
        path = ".".join(parts)
        return self.get(path, default, cast=cast)

    # ---------- file helpers you may want later ----------
    @staticmethod
    def list_yaml_files(root_dir: str) -> List[str]:
        root = Path(_normalize_dbfs_like(root_dir))
        if not root.is_absolute():
            root = (Path.cwd() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise FileNotFoundError(f"Folder not found or not a directory: {root}")
        out: List[str] = []
        for cur, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                if _is_yaml(f):
                    out.append(str(Path(cur) / f))
        return sorted(out)


# ---------- optional 1-liner loader (snapshot + FQN) ----------
def load_config_and_fqn(
    config_path: str,
    *,
    variables: Optional[Dict[str, str]] = None,
    notebook_idx: Optional[int] = None,
    target_idx: Optional[int] = None,
) -> tuple[ProjectConfig, Optional[str]]:
    """
    Convenience for jobs that want a one-liner:
      cfg, fqn = load_config_and_fqn(".../dqx_config.yaml", variables={"env": "dev"}, notebook_idx=2, target_idx=1)

    If notebook_idx/target_idx are provided, returns the target FQN as the second tuple item; otherwise None.
    """
    cfg = ProjectConfig(config_path, variables=variables)
    fqn = None
    if notebook_idx is not None and target_idx is not None:
        fqn = cfg.table_fqn(notebook_idx, target_idx)
    return cfg, fqn