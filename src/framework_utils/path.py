# utils/path.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List

# ----------------------------
# Project / environment helpers
# ----------------------------
def find_project_root(start_path: str) -> Path:
    path = Path(start_path).resolve()
    for parent in [path] + list(path.parents):
        if (parent / "pyproject.toml").exists() or (parent / "setup.py").exists():
            return parent
    raise RuntimeError("Project root not found (no pyproject.toml or setup.py)")

def _expand_user_env(p: str) -> str:
    return os.path.expandvars(os.path.expanduser(p or ""))

# ----------------------------
# Cloud / DBFS helpers
# ----------------------------
_CLOUD_SCHEMES = ("abfss://", "s3://", "gs://", "wasbs://", "adl://", "oss://", "cos://")

def is_cloud_uri(p: str) -> bool:
    return isinstance(p, str) and p.startswith(_CLOUD_SCHEMES)

# dbfs:/... → /dbfs/... (so open() works). Leave non-DBFS URIs alone
def dbfs_to_local(p: str) -> str:
    if not isinstance(p, str):
        return p
    if p.startswith("dbfs:/"):
        return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
    if p.startswith("file:/dbfs/"):
        return "/" + p[len("file:/"):].lstrip("/")
    return p

# ----------------------------
# Normalization & resolution
# ----------------------------
def normalize_path(
    p: str,
    *,
    base: Optional[Path] = None,
    start_file: Optional[str] = None,
) -> str:
    """
    Normalize a path for local filesystem use:
      - expands ~ and $VARS
      - converts DBFS URIs to /dbfs/ local mount
      - preserves cloud URIs (abfss://, s3://, etc.)
      - resolves relative paths against `base` or the project root of `start_file`
    """
    if not p:
        return p
    p = _expand_user_env(p)

    if is_cloud_uri(p):
        return p  # let Spark handle cloud URIs directly

    p = dbfs_to_local(p)
    path_obj = Path(p)

    if not path_obj.is_absolute():
        if base:
            path_obj = Path(base) / path_obj
        elif start_file:
            pr = find_project_root(start_file)
            path_obj = pr / path_obj
        else:
            path_obj = path_obj.resolve()

    return str(path_obj)

def resolve_resource(
    rel_or_abs: str,
    *,
    start_file: Optional[str] = None,
    project_root: Optional[Path] = None,
) -> str:
    """
    Resolve a repo resource path (e.g., 'resources/...') using either
    an explicit project_root or by discovering from start_file.
    """
    base = project_root if project_root is not None else None
    return normalize_path(rel_or_abs, base=base, start_file=start_file)

def resolve_relative_to_file(target_path: str, *, base_file: str) -> str:
    """
    Resolve `target_path` relative to the directory containing `base_file`.
    - Honors DBFS conversion
    - Leaves cloud URIs intact
    """
    if not target_path:
        return target_path
    if is_cloud_uri(target_path):
        return target_path
    base_dir = Path(dbfs_to_local(base_file)).resolve().parent
    return normalize_path(target_path, base=base_dir)

# ----------------------------
# Simple YAML discovery (local/DBFS)
# ----------------------------
def list_yaml_files(root: str, *, include_hidden: bool = False) -> List[str]:
    """
    Recursively list .yaml/.yml under `root`. Supports local & DBFS (/dbfs or dbfs:/).
    Cloud URIs are not walked here (return [] or raise if needed).
    """
    if not root:
        return []
    if is_cloud_uri(root):
        # Walking cloud storage from driver isn't supported here; use Spark APIs instead.
        return []

    root_local = Path(dbfs_to_local(root))
    if root_local.is_file():
        low = root_local.name.lower()
        return [str(root_local)] if (low.endswith(".yaml") or low.endswith(".yml")) else []

    if not root_local.exists() or not root_local.is_dir():
        raise FileNotFoundError(f"Path not found or not a directory: {root_local}")

    out: List[str] = []
    for cur, dirs, files in os.walk(root_local):
        if not include_hidden:
            dirs[:] = [d for d in dirs if not d.startswith(".")]
        for f in files:
            if not include_hidden and f.startswith("."):
                continue
            low = f.lower()
            if low.endswith(".yaml") or low.endswith(".yml"):
                out.append(str(Path(cur) / f))
    out.sort()
    return out