# utils/path.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def find_project_root(start_path: str) -> Path:
    path = Path(start_path).resolve()
    for parent in [path] + list(path.parents):
        if (parent / "pyproject.toml").exists() or (parent / "setup.py").exists():
            return parent
    raise RuntimeError("Project root not found (no pyproject.toml or setup.py)")


# light env/home expansion
def _expand_user_env(p: str) -> str:
    return os.path.expandvars(os.path.expanduser(p or ""))


# dbfs:/... → /dbfs/... (so open() works). leave non-DBFS URIs.
def dbfs_to_local(p: str) -> str:
    if p.startswith("dbfs:/"):
        return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
    if p.startswith("file:/dbfs/"):
        return "/" + p[len("file:/"):].lstrip("/")
    return p


# normalize a path for local use; keep cloud URIs unchanged
def normalize_path(
    p: str,
    *,
    base: Optional[Path] = None,
    start_file: Optional[str] = None,
) -> str:
    if not p:
        return p
    p = _expand_user_env(p)

    # keep cloud URIs as-is; handle with Spark rather than open()
    if p.startswith(("abfss://", "s3://", "gs://")):
        return p

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


# helper for repo resources (e.g. "resources/…")
def resolve_resource(
    rel_or_abs: str,
    *,
    start_file: Optional[str] = None,
    project_root: Optional[Path] = None,
) -> str:
    if project_root is None and start_file is not None:
        project_root = find_project_root(start_file)
    base = project_root if project_root is not None else None
    return normalize_path(rel_or_abs, base=base)