I dont know what the fuck youre doing now, but i get this errro: 
FileNotFoundError: Rules folder not found or not a directory: /Workspace/Users/levi.gagne@claconnect.com/DQX/src/dqx/resources/resources/dqx_checks_config_load
File <command-4730582016129069>, line 804
    791     return main(
    792         output_config_path=dqx_cfg_yaml,
    793         rules_dir=None,
   (...)
    800         created_by=created_by,
    801     )
    803 # ---- run it ----
--> 804 res = load_checks(
    805     dqx_cfg_yaml="resources/dqx_config.yaml",
    806     created_by="AdminUser",
    807     # dry_run=True,
    808     # validate_only=True,
    809     batch_dedupe_mode="warn",
    810 )
    811 print(res)
File /Workspace/Users/levi.gagne@claconnect.com/DQX/src/dqx/utils/config.py:81, in ProjectConfig.list_rule_files(self, base_dir)
     79     root = (self._base_dir / root).resolve()
     80 if not root.exists() or not root.is_dir():
---> 81     raise FileNotFoundError(f"Rules folder not found or not a directory: {root}")
     83 out: List[str] = []
     84 for cur, dirs, files in os.walk(root):




Im realzing you messed up the config.py relly bad


First off, I was passing this path to the file arleady:
No need to make it overly complex


and remember the main point of creating config.py was to replace these functions: 
# =========================
# YAML loading (robust)
# =========================
def load_yaml_rules(path: str) -> List[dict]:
    with open(path, "r") as fh:
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

and this: 
# =========================
# Recursive discovery + write
# =========================
def _load_output_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return yaml.safe_load(fh) or {}

def _parse_checks_table_cfg(cfg_value: Any) -> Tuple[str, str]:
    """
    Supports:
      - String (legacy):  "dq_dev.dqx.checks_config" → (name, 'check_id')
      - Mapping (new):    {name: "...", primary_key: "check_id"} → (name, pk)
    """
    if isinstance(cfg_value, dict):
        nm = cfg_value.get("name") or cfg_value.get("table") or cfg_value.get("table_name")
        if not nm:
            raise ValueError("dqx_checks_config_table_name must include 'name' when provided as a mapping.")
        pk = cfg_value.get("primary_key", "check_id")
        return str(nm), str(pk)




    please show me teh updated config.py file please: 
    # src/dqx/utils/config.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pyspark.sql import SparkSession


def _normalize_dbfs_like(p: str) -> str:
    # Accepts "dbfs:/..." or normal paths; returns a local-style path.
    if p.startswith("dbfs:/"):
        return "/dbfs/" + p[len("dbfs:/"):].lstrip("/")
    return p


def _is_yaml(fname: str) -> bool:
    low = fname.lower()
    return low.endswith(".yaml") or low.endswith(".yml")


class ProjectConfig:
    """
    Minimal, notebook-friendly YAML config loader.

    - Resolves relative paths against the directory of the config file itself.
    - No project-root scanning or __file__ required.
    """

    def __init__(self, config_path: str, spark: Optional[SparkSession] = None):
        cfg_path = Path(_normalize_dbfs_like(config_path))
        if not cfg_path.is_absolute():
            cfg_path = (Path.cwd() / cfg_path).resolve()
        if not cfg_path.exists():
            raise FileNotFoundError(f"Config not found: {cfg_path}")

        self._cfg_path = cfg_path
        self._base_dir = self._cfg_path.parent
        self.spark = spark

        with open(self._cfg_path, "r") as fh:
            self._cfg: Dict[str, Any] = yaml.safe_load(fh) or {}

    @property
    def raw(self) -> Dict[str, Any]:
        return self._cfg

    def yaml_rules_dir(self) -> str:
        # dqx_yaml_checks: relative to the config file’s folder
        rules_dir = str(self._cfg.get("dqx_yaml_checks", "")).strip()
        if not rules_dir:
            raise ValueError("Missing 'dqx_yaml_checks' in config.")
        rules_dir = _normalize_dbfs_like(rules_dir)
        p = Path(rules_dir)
        if not p.is_absolute():
            p = (self._base_dir / p).resolve()
        return str(p)

    def checks_config_table(self) -> (str, str):
        # dqx_checks_config_table_name: can be "catalog.schema.table" or mapping {name, primary_key}
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
        # Recursively discover *.yaml / *.yml under base_dir (resolved relative to config file)
        root = Path(_normalize_dbfs_like(base_dir))
        if not root.is_absolute():
            root = (self._base_dir / root).resolve()
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
    return str(cfg_value), "check_id"


