Now this next ask is going to be a big lift


Here is current version of the config file I have:
#######################################################
# src/dqx/resources/dqx_config.yaml

# 01_load_dqx_checks
dqx_yaml_checks: resources/dqx_checks_config_load
dqx_checks_config_table_name:
  name: dq_dev.dqx.checks_config
  primary_key: check_id

# 02_run_dqx_checks
dqx_checks_log_table_name: dq_dev.dqx.checks_log


run_config_name:
  default:
    output_config: 
      mode: overwrite
      options:
        mergeSchema: true



and the current config.py we use to grab those values is here:
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


and here is my current dictionary: 
# ======================================================
# Documentation dictionary
# ======================================================
DQX_CHECKS_CONFIG_METADATA: Dict[str, Any] = {
    "table": "<override at create time>",
    "table_comment": (
        "## DQX Checks Configuration\n"
        "- One row per **unique canonical rule** generated from YAML (source of truth).\n"
        "- **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.\n"
        "- Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.\n"
        "- Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.\n"
        "- `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.\n"
        "- `run_config_name` is a **routing tag**, not part of identity.\n"
        "- Only rows with `active=true` are executed."
    ),
    "columns": {
        "check_id": "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}.",
        "check_id_payload": "Canonical JSON used to derive `check_id` (sorted keys, normalized values).",
        "table_name": "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability.",
        "name": "Human-readable rule name. Used in UI/diagnostics and name-based joins when enriching logs.",
        "criticality": "Rule severity: `warn|warning|error`. Reporting normalizes warn/warning → `warning`.",
        "check": "Structured rule: `{function, for_each_column?, arguments?}`; argument values stringified.",
        "filter": "Optional SQL predicate applied before evaluation (row-level). Normalized in payload.",
        "run_config_name": "Execution group/tag. Drives which runs pick up this rule; **not** part of identity.",
        "user_metadata": "Free-form `map<string,string>` carried through to issues for traceability.",
        "yaml_path": "Absolute/volume path to the defining YAML doc (lineage).",
        "active": "If `false`, rule is ignored by runners.",
        "created_by": "Audit: creator/principal that materialized the row.",
        "created_at": "Audit: creation timestamp (cast to TIMESTAMP on write).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (nullable; cast to TIMESTAMP on write).",
    },
}


I expanded the yaml, with a rough format if you could hlep me with it.

# src/dqx/resources/dqx_config.yaml

dqx_config:
  local_timezone: America/Chicago
  processing_timezone: UTC
  apply_table_metadata: False

# 01_load_dqx_checks
dqx_load_checks:
  checks_config_source:
    storage_type: repo # 'repo' | 'volume'
    source_format: yaml # 'yaml' | 'json' | 'python'
    source_path: resources/dqx_checks_config_load
    allowed_criticality: [error, warn]
    required_fields: [table_name, name, criticality, run_config_name, check]
  checks_config_table:
    target_table: dq_{env}.dqx.checks_config
      table_format: delta
      write_mode: overwrite
      primary_key: check_id
      partition_by: {}
      table_description: |
        "## DQX Checks Configuration\n"
        "- One row per **unique canonical rule** generated from YAML (source of truth).\n"
        "- **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.\n"
        "- Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.\n"
        "- Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.\n"
        "- `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.\n"
        "- `run_config_name` is a **routing tag**, not part of identity.\n"
        "- Only rows with `active=true` are executed."
      columns:
        - check_id:
          - datatype: StringType
          - nullable: False
          - allowed_values: [] # e.g. criticality - [error, warn]
          - column_description: "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}."

# 02_run_dqx_checks
dqx_run_checks:
  checks_log_table:
    target_table: dq_{env}.dqx.checks_log
      target_format: delta
      write_mode: overwrite
      primary_key: log_id
      partition_by: N/A
  checks_log_summary_by_rule_table:
    target_table: dq_{env}.dqx.checks_log_summary_by_rule
      target_format: delta
      write_mode: overwrite
      primary_key:
      partition_by: N/A
  checks_log_summary_by_table_table:
    target_table: dq_{env}.dqx.checks_log_summary_by_table
      target_format: delta
      write_mode: overwrite
      primary_key:
      partition_by: N/A
      created_by_default: AdminUser


run_config_name:
  default:
    output_config: (note: i would like to call checks_log_table-target_table value defined above not sure the best way tot do that with yaml or if we should just repeat here)
      location:
        mode: overwrite
        format: delta
        options:
          mergeSchema: true
    quarantine_config:
      location: dq_{env}.dqx.checks_quarantine
        mode: append
        format: delta
        options:
          mergeSchema: true
  generated_check:
    output_config:
      location: dq_{env}.dqx.generated_checks_log
        mode: overwrite
        format: delta
        options:
          mergeSchema: true
    quarantine_config:
      location: dq_{env}.dqx.generated_checks_quarantine
        mode: append
        format: delta
        options:
          mergeSchema: true



Now youll notice that i expanded to include more variables, what I would liek as well is so that all of the information int he dictionaries is in this file


so we would have a column for each one, with the description, 

do you think you can update the yaml based on these dictionaries, which should go under
checks_config_table:
DQX_CHECKS_CONFIG_METADATA: Dict[str, Any] = {
    "table": "<override at create time>",
    "table_comment": (
        "## DQX Checks Configuration\n"
        "- One row per **unique canonical rule** generated from YAML (source of truth).\n"
        "- **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.\n"
        "- Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.\n"
        "- Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.\n"
        "- `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.\n"
        "- `run_config_name` is a **routing tag**, not part of identity.\n"
        "- Only rows with `active=true` are executed."
    ),
    "columns": {
        "check_id": "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}.",
        "check_id_payload": "Canonical JSON used to derive `check_id` (sorted keys, normalized values).",
        "table_name": "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability.",
        "name": "Human-readable rule name. Used in UI/diagnostics and name-based joins when enriching logs.",
        "criticality": "Rule severity: `warn|warning|error`. Reporting normalizes warn/warning → `warning`.",
        "check": "Structured rule: `{function, for_each_column?, arguments?}`; argument values stringified.",
        "filter": "Optional SQL predicate applied before evaluation (row-level). Normalized in payload.",
        "run_config_name": "Execution group/tag. Drives which runs pick up this rule; **not** part of identity.",
        "user_metadata": "Free-form `map<string,string>` carried through to issues for traceability.",
        "yaml_path": "Absolute/volume path to the defining YAML doc (lineage).",
        "active": "If `false`, rule is ignored by runners.",
        "created_by": "Audit: creator/principal that materialized the row.",
        "created_at": "Audit: creation timestamp (cast to TIMESTAMP on write).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (nullable; cast to TIMESTAMP on write).",
    },
}


checks_log_table:
DQX_CHECKS_LOG_METADATA: Dict[str, Any] = {
    "table": "dq_dev.dqx.checks_log",
    "table_comment": (
        "## DQX Row-Level Check Results Log\n"
        "- One row per **flagged source row** (error or warning) emitted by DQX for a given run.\n"
        "- **Primary key**: `log_id` = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp). "
        "**No duplicates allowed** within a run; duplicates indicate a repeated write or misconfigured mode.\n"
        "- `_errors/_warnings` capture issue structs; corresponding fingerprints are deterministic and order-insensitive.\n"
        "- `row_snapshot` captures the offending row’s non-reserved columns (values stringified) at evaluation time.\n"
        "- `check_id` lists originating rule IDs from the config table; may be empty if name-based mapping was not possible.\n"
        "- Writers should ensure idempotency (e.g., overwrite mode or dedupe by `log_id` when appending)."
    ),
    "columns": {
        "log_id": "PRIMARY KEY. Deterministic sha256 over (table_name, run_config_name, row_snapshot_fingerprint, _errors_fingerprint, _warnings_fingerprint).",
        "check_id": "Array of originating config `check_id`s that fired for this row (may be empty if unmapped).",
        "table_name": "Source table FQN (`catalog.schema.table`) where the row was evaluated.",
        "run_config_name": "Run configuration under which the checks executed.",
        "_errors": "Array<struct> of error issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_errors_fingerprint": "Deterministic digest of normalized `_errors` (order/column-order insensitive).",
        "_warnings": "Array<struct> of warning issues {name, message, columns, filter, function, run_time, user_metadata}.",
        "_warnings_fingerprint": "Deterministic digest of normalized `_warnings`.",
        "row_snapshot": "Array<struct{column:string, value:string}> of non-reserved columns for the flagged row (stringified).",
        "row_snapshot_fingerprint": "sha256(JSON(row_snapshot)) used in `log_id` and de-duplication.",
        "created_by": "Audit: writer identity for this record.",
        "created_at": "Audit: creation timestamp (UTC).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (UTC, nullable).",
    },
}



YOulll also notice that for the run_config_name i expanded,
                       id rather show all opotions and have them as empty, so can you hlep with this?


Asa  reminder, the config.py is used to grab the values from the yaml. so for example if the documentation.py wants to grab the comment description or table description is should do so thorught the the config.py which is here:
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





You will also need to updat this file:
# src/dqx/utils/documentation.py

from __future__ import annotations

import json
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from utils.display import show_df, display_section

__all__ = [
    "_materialize_table_doc",
    "_q_fqn",
    "_esc_comment",
    "preview_table_documentation",
    "apply_table_documentation",
]


def _materialize_table_doc(doc_template: Dict[str, Any], table_fqn: str) -> Dict[str, Any]:
    """Fill the table_fqn into the doc template (deep copied), and expand {TABLE_FQN} placeholders."""
    copy = json.loads(json.dumps(doc_template))
    copy["table"] = table_fqn
    if "table_comment" in copy and isinstance(copy["table_comment"], str):
        copy["table_comment"] = copy["table_comment"].replace("{TABLE_FQN}", table_fqn)
    return copy


def _q_fqn(fqn: str) -> str:
    """Backtick-quote catalog.schema.table."""
    return ".".join(f"`{p}`" for p in fqn.split("."))


# =========================
# Comments + documentation
# =========================
def _esc_comment(s: str) -> str:
    """Escape single quotes for SQL COMMENT strings."""
    return (s or "").replace("'", "''")


def preview_table_documentation(spark: SparkSession, table_fqn: str, doc: Dict[str, Any]) -> None:
    display_section("TABLE METADATA PREVIEW (markdown text stored in comments)")
    doc_df = spark.createDataFrame(
        [(table_fqn, doc.get("table_comment", ""))],
        schema="table string, table_comment_markdown string",
    )
    show_df(doc_df, n=1, truncate=False)

    cols = doc.get("columns", {}) or {}
    cols_df = spark.createDataFrame(
        [(k, v) for k, v in cols.items()],
        schema="column string, column_comment_markdown string",
    )
    show_df(cols_df, n=200, truncate=False)


def apply_table_documentation(
    spark: SparkSession,
    table_fqn: str,
    doc: Optional[Dict[str, Any]],
) -> None:
    """Apply table comment and per-column comments, with fallbacks for engines that lack COMMENT support."""
    if not doc:
        return

    qtable = _q_fqn(table_fqn)

    # Table comment
    table_comment = _esc_comment(doc.get("table_comment", ""))
    if table_comment:
        try:
            spark.sql(f"COMMENT ON TABLE {qtable} IS '{table_comment}'")
        except Exception:
            # Fallback to TBLPROPERTIES for engines that don’t support COMMENT
            spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    # Column comments (always best-effort)
    cols: Dict[str, str] = doc.get("columns", {}) or {}
    existing_cols = {f.name.lower() for f in spark.table(table_fqn).schema.fields}
    for col_name, comment in cols.items():
        if col_name.lower() not in existing_cols:
            continue
        qcol = f"`{col_name}`"
        comment_sql = f"ALTER TABLE {qtable} ALTER COLUMN {qcol} COMMENT '{_esc_comment(comment)}'"
        try:
            spark.sql(comment_sql)
        except Exception:
            # Try COMMENT ON COLUMN as fallback
            try:
                spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{_esc_comment(comment)}'")
            except Exception as e2:
                print(f"[meta] Skipped column comment for {table_fqn}.{col_name}: {e2}")







So please show me the updated config.py, documentation.py and dqx_config.yaml


here is the notebook to update as well:
import json
import hashlib
from typing import Dict, Any, Optional, List

from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

from pyspark.sql import SparkSession, DataFrame, types as T
from pyspark.sql.functions import to_timestamp, col, desc

from databricks.labs.dqx.engine import DQEngine

from utils.display import show_df, display_section
from utils.runtime import print_notebook_env
from utils.color import Color
from utils.config import ProjectConfig
from utils.table import (
    table_exists,
    add_primary_key_constraint,
    write_empty_delta_table,
    empty_df_from_schema,
    create_table_if_absent,
)
from utils.documentation import (
    _materialize_table_doc,
    _q_fqn,
    _esc_comment,
    preview_table_documentation,
    apply_table_documentation,
)

from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED

try:
    THIS_FILE = __file__
except NameError:
    THIS_FILE = None

# =========================
# Target schema (Delta sink)
# =========================
TABLE_SCHEMA = T.StructType([
    T.StructField("check_id",            T.StringType(), False),
    T.StructField("check_id_payload",    T.StringType(), False),
    T.StructField("table_name",          T.StringType(), False),

    # DQX fields
    T.StructField("name",                T.StringType(), False),
    T.StructField("criticality",         T.StringType(), False),
    T.StructField("check", T.StructType([
        T.StructField("function",        T.StringType(), False),
        T.StructField("for_each_column", T.ArrayType(T.StringType()), True),
        T.StructField("arguments",       T.MapType(T.StringType(), T.StringType()), True),
    ]), False),
    T.StructField("filter",              T.StringType(), True),
    T.StructField("run_config_name",     T.StringType(), False),
    T.StructField("user_metadata",       T.MapType(T.StringType(), T.StringType()), True),

    # Ops fields
    T.StructField("yaml_path",           T.StringType(), False),
    T.StructField("active",              T.BooleanType(), False),
    T.StructField("created_by",          T.StringType(), False),
    T.StructField("created_at",          T.StringType(), False),
    T.StructField("updated_by",          T.StringType(), True),
    T.StructField("updated_at",          T.StringType(), True),
])

# ======================================================
# Documentation dictionary
# ======================================================
DQX_CHECKS_CONFIG_METADATA: Dict[str, Any] = {
    "table": "<override at create time>",
    "table_comment": (
        "## DQX Checks Configuration\n"
        "- One row per **unique canonical rule** generated from YAML (source of truth).\n"
        "- **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.\n"
        "- Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.\n"
        "- Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.\n"
        "- `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.\n"
        "- `run_config_name` is a **routing tag**, not part of identity.\n"
        "- Only rows with `active=true` are executed."
    ),
    "columns": {
        "check_id": "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}.",
        "check_id_payload": "Canonical JSON used to derive `check_id` (sorted keys, normalized values).",
        "table_name": "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability.",
        "name": "Human-readable rule name. Used in UI/diagnostics and name-based joins when enriching logs.",
        "criticality": "Rule severity: `warn|warning|error`. Reporting normalizes warn/warning → `warning`.",
        "check": "Structured rule: `{function, for_each_column?, arguments?}`; argument values stringified.",
        "filter": "Optional SQL predicate applied before evaluation (row-level). Normalized in payload.",
        "run_config_name": "Execution group/tag. Drives which runs pick up this rule; **not** part of identity.",
        "user_metadata": "Free-form `map<string,string>` carried through to issues for traceability.",
        "yaml_path": "Absolute/volume path to the defining YAML doc (lineage).",
        "active": "If `false`, rule is ignored by runners.",
        "created_by": "Audit: creator/principal that materialized the row.",
        "created_at": "Audit: creation timestamp (cast to TIMESTAMP on write).",
        "updated_by": "Audit: last updater (nullable).",
        "updated_at": "Audit: last update timestamp (nullable; cast to TIMESTAMP on write).",
    },
}

# =========================
# Canonicalization & IDs
# =========================
def _canon_filter(s: Optional[str]) -> str:
    return "" if not s else " ".join(str(s).split())

def _canon_check(chk: Dict[str, Any]) -> Dict[str, Any]:
    out = {"function": chk.get("function"), "for_each_column": None, "arguments": {}}
    fec = chk.get("for_each_column")
    if isinstance(fec, list):
        out["for_each_column"] = sorted([str(x) for x in fec]) or None
    args = chk.get("arguments") or {}
    canon_args: Dict[str, str] = {}
    for k, v in args.items():
        sv = "" if v is None else str(v).strip()
        if (sv.startswith("{") and sv.endswith("}")) or (sv.startswith("[") and sv.endswith("]")):
            try:
                sv = json.dumps(json.loads(sv), sort_keys=True, separators=(",", ":"))
            except Exception:
                pass
        canon_args[str(k)] = sv
    out["arguments"] = {k: canon_args[k] for k in sorted(canon_args)}
    return out

def compute_check_id_payload(table_name: str, check_dict: Dict[str, Any], filter_str: Optional[str]) -> str:
    payload_obj = {
        "table_name": (table_name or "").lower(),
        "filter": _canon_filter(filter_str),
        "check": _canon_check(check_dict or {}),
    }
    return json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))

def compute_check_id_from_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()

# =========================
# Conversions / validation
# =========================
def _stringify_map_values(d: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (d or {}).items():
        if isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif v is None:
            out[k] = "null"
        else:
            out[k] = str(v)
    return out

def validate_rules_file(rules: List[dict], file_path: str):
    if not rules:
        raise ValueError(f"No rules found in {file_path} (empty or invalid YAML).")
    probs, seen = [], set()
    for r in rules:
        nm = r.get("name")
        if not nm:
            probs.append(f"Missing rule name in {file_path}")
        if nm in seen:
            probs.append(f"Duplicate rule name '{nm}' in {file_path}")
        seen.add(nm)
    if probs:
        raise ValueError(f"File-level validation failed in {file_path}: {probs}")

def validate_rule_fields(
    rule: dict,
    file_path: str,
    required_fields: List[str],
    allowed_criticality={"error", "warn", "warning"},
):
    probs = []
    for f in required_fields:
        if not rule.get(f):
            probs.append(f"Missing required field '{f}' in rule '{rule.get('name')}' ({file_path})")
    if rule.get("table_name", "").count(".") != 2:
        probs.append(
            f"table_name '{rule.get('table_name')}' not fully qualified in rule '{rule.get('name')}' ({file_path})"
        )
    if rule.get("criticality") not in allowed_criticality:
        probs.append(
            f"Invalid criticality '{rule.get('criticality')}' in rule '{rule.get('name')}' ({file_path})"
        )
    if not rule.get("check", {}).get("function"):
        probs.append(f"Missing check.function in rule '{rule.get('name')}' ({file_path})")
    if probs:
        raise ValueError("Rule-level validation failed: " + "; ".join(probs))

def validate_with_dqx(rules: List[dict], file_path: str):
    status = DQEngine.validate_checks(rules)
    if getattr(status, "has_errors", False):
        raise ValueError(f"DQX validation failed in {file_path}:\n{status.to_string()}")

# =========================
# Build rows
# =========================
def process_yaml_file(path: str, required_fields: List[str], time_zone: str = "UTC", created_by: str = "AdminUser") -> List[dict]:
    docs = ProjectConfig.load_yaml_rules(path)
    if not docs:
        print(f"[skip] {path} has no rules.")
        return []

    validate_rules_file(docs, path)
    flat: List[dict] = []

    for rule in docs:
        validate_rule_fields(rule, path, required_fields=required_fields)

        raw_check = rule["check"] or {}
        payload = compute_check_id_payload(rule["table_name"], raw_check, rule.get("filter"))
        check_id = compute_check_id_from_payload(payload)

        function = raw_check.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"{path}: check.function must be a non-empty string (rule '{rule.get('name')}').")

        for_each = raw_check.get("for_each_column")
        if for_each is not None and not isinstance(for_each, list):
            raise ValueError(f"{path}: check.for_each_column must be an array of strings (rule '{rule.get('name')}').")
        if isinstance(for_each, list):
            try:
                for_each = [str(x) for x in for_each]
            except Exception:
                raise ValueError(f"{path}: unable to cast for_each_column items to strings (rule '{rule.get('name')}').")

        arguments = raw_check.get("arguments", {}) or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{path}: check.arguments must be a map (rule '{rule.get('name')}').")
        arguments = _stringify_map_values(arguments)

        user_metadata = rule.get("user_metadata")
        if user_metadata is not None:
            if not isinstance(user_metadata, dict):
                raise ValueError(f"{path}: user_metadata must be a map (rule '{rule.get('name')}').")
            user_metadata = _stringify_map_values(user_metadata)

        # ---- inline timestamp (no helper) ----
        created_at_iso = (
            datetime.now(ZoneInfo(time_zone)).isoformat()
            if ZoneInfo is not None else
            datetime.now(timezone.utc).isoformat()
        )

        flat.append(
            {
                "check_id": check_id,
                "check_id_payload": payload,
                "table_name": rule["table_name"],
                "name": rule["name"],
                "criticality": rule["criticality"],
                "check": {
                    "function": function,
                    "for_each_column": for_each if for_each else None,
                    "arguments": arguments if arguments else None,
                },
                "filter": rule.get("filter"),
                "run_config_name": rule["run_config_name"],
                "user_metadata": user_metadata if user_metadata else None,
                "yaml_path": path,
                "active": rule.get("active", True),
                "created_by": created_by,
                "created_at": created_at_iso,  # <— inline
                "updated_by": None,
                "updated_at": None,
            }
        )

    validate_with_dqx(docs, path)
    return flat

# =========================
# Batch dedupe (on check_id ONLY)
# =========================
def _fmt_rule_for_dup(r: dict) -> str:
    return (
        f"name={r.get('name')} | file={r.get('yaml_path')} | "
        f"criticality={r.get('criticality')} | run_config={r.get('run_config_name')} | "
        f"filter={r.get('filter')}"
    )

def dedupe_rules_in_batch_by_check_id(rules: List[dict], mode: str = "warn") -> List[dict]:
    groups: Dict[str, List[dict]] = {}
    for r in rules:
        groups.setdefault(r["check_id"], []).append(r)

    out: List[dict] = []
    dropped = 0
    blocks: List[str] = []

    for cid, lst in groups.items():
        if len(lst) == 1:
            out.append(lst[0]); continue
        lst = sorted(lst, key=lambda x: (x.get("yaml_path", ""), x.get("name", "")))
        keep, dups = lst[0], lst[1:]
        dropped += len(dups)
        head = f"[dup/batch/check_id] {len(dups)} duplicate(s) for check_id={cid[:12]}…"
        lines = ["    " + _fmt_rule_for_dup(x) for x in lst]
        tail = f"    -> keeping: name={keep.get('name')} | file={keep.get('yaml_path')}"
        blocks.append("\n".join([head, *lines, tail]))
        out.append(keep)

    if dropped:
        msg = "\n\n".join(blocks) + f"\n[dedupe/batch] total dropped={dropped}"
        if mode == "error":
            raise ValueError(msg)
        if mode == "warn":
            print(msg)
    return out


# =========================
# Data overwrite (preserve metadata)
# =========================
def overwrite_rules_into_delta(
    spark: SparkSession,
    df: DataFrame,
    delta_table_name: str,
    table_doc: Optional[Dict[str, Any]] = None,  # kept for signature compatibility
    *,
    primary_key: str = "check_id",
):
    """
    Replace table data while preserving table properties, comments, and constraints.
    """
    df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at")))

    if not table_exists(spark, delta_table_name):
        # Safety: if not created yet (shouldn't happen given main), create now.
        create_table_if_absent(
            spark, delta_table_name, schema=TABLE_SCHEMA,
            table_doc=_materialize_table_doc(DQX_CHECKS_CONFIG_METADATA, delta_table_name),
            primary_key=primary_key
        )

    # Align column order to the existing table to keep INSERT OVERWRITE happy.
    target_schema = spark.table(delta_table_name).schema
    target_cols = [f.name for f in target_schema.fields]
    missing = [c for c in target_cols if c not in df.columns]
    extra   = [c for c in df.columns if c not in target_cols]
    if missing or extra:
        raise ValueError(
            f"Schema mismatch for INSERT OVERWRITE.\n"
            f"Missing in df: {missing}\nExtra in df: {extra}"
        )

    df_ordered = df.select(*target_cols)
    tmp_view = "__tmp_overwrite_rules"
    df_ordered.createOrReplaceTempView(tmp_view)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {_q_fqn(delta_table_name)} SELECT * FROM {tmp_view}")
    finally:
        try:
            spark.catalog.dropTempView(tmp_view)
        except Exception:
            pass

    display_section("WRITE RESULT (Delta)")
    summary = spark.createDataFrame(
        [(df.count(), delta_table_name, "insert overwrite", f"pk_{primary_key}")],
        schema="`rules written` long, `target table` string, `mode` string, `constraint` string",
    )
    show_df(summary, n=1)
    print(
        f"\n{Color.b}{Color.ivory}Rules written to: "
        f"'{Color.r}{Color.b}{Color.chartreuse}{delta_table_name}{Color.r}{Color.b}{Color.ivory}' "
        f"(metadata preserved){Color.r}"
    )

# =========================
# Display-first debug helpers
# =========================
def debug_display_batch(spark: SparkSession, df_rules: DataFrame) -> None:
    display_section("SUMMARY OF RULES LOADED FROM YAML")
    totals = [
        (
            df_rules.count(),
            df_rules.select("check_id").distinct().count(),
            df_rules.select("check_id", "run_config_name").distinct().count(),
        )
    ]
    totals_df = spark.createDataFrame(
        totals,
        schema="`total number of rules found` long, `unique rules found` long, `distinct pair of rules` long",
    )
    show_df(totals_df, n=1)

    display_section("SAMPLE OF RULES LOADED FROM YAML (check_id, name, run_config_name, yaml_path)")
    sample_cols = df_rules.select("check_id", "name", "run_config_name", "yaml_path").orderBy(desc("yaml_path"))
    show_df(sample_cols, n=50, truncate=False)

    display_section("RULES LOADED PER TABLE")
    by_table = df_rules.groupBy("table_name").count().orderBy(desc("count"))
    show_df(by_table, n=200)

def print_rules_df(spark: SparkSession, rules: List[dict]) -> Optional[DataFrame]:
    if not rules:
        print("No rules to show.")
        return None
    df = (
        spark.createDataFrame(rules, schema=TABLE_SCHEMA)
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("updated_at", to_timestamp(col("updated_at")))
    )
    debug_display_batch(spark, df)
    return df

# =========================
# Main
# =========================
def main(
    output_config_path: str = "resources/dqx_config.yaml",
    rules_dir: Optional[str] = None,
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False,
    required_fields: Optional[List[str]] = None,
    batch_dedupe_mode: str = "warn",
    table_doc: Optional[Dict[str, Any]] = None,
    created_by: str = "AdminUser",
    apply_table_metadata: bool = False,
):
    spark = SparkSession.builder.getOrCreate()

    # Simple: use the provided time_zone as-is.
    print_notebook_env(spark, local_timezone=time_zone)

    cfg = ProjectConfig(output_config_path, spark=spark)
    rules_dir = rules_dir or cfg.yaml_rules_dir()
    delta_table_name, primary_key = cfg.checks_config_table()

    required_fields = required_fields or ["table_name", "name", "criticality", "run_config_name", "check"]

    # Create-once / metadata gating
    doc = _materialize_table_doc(table_doc or DQX_CHECKS_CONFIG_METADATA, delta_table_name)
    exists = table_exists(spark, delta_table_name)
    if not exists:
        create_table_if_absent(
            spark,
            delta_table_name,
            schema=TABLE_SCHEMA,
            table_doc=doc,
            primary_key=primary_key,
            partition_by=None,
        )
    elif apply_table_metadata:
        apply_table_documentation(spark, delta_table_name, doc)

    # Discover YAML files
    yaml_files = cfg.list_rule_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    files_df = spark.createDataFrame([(p,) for p in yaml_files], "yaml_path string")
    show_df(files_df, n=500, truncate=False)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        errs: List[str] = []
        for p in yaml_files:
            try:
                validate_rules_file(ProjectConfig.load_yaml_rules(p), p)
            except Exception as e:
                errs.append(f"{p}: {e}")
        return {
            "mode": "validate_only",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "errors": errs,
        }

    # Collect rules
    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(
            full_path,
            required_fields=required_fields,
            time_zone=time_zone,
            created_by=created_by
        )
        if file_rules:
            all_rules.extend(file_rules)
            print(f"[loader] {full_path}: rules={len(file_rules)}")

    if not all_rules:
        print("No rules discovered; nothing to do.")
        return {"mode": "no_op", "config_path": output_config_path, "rules_files": len(yaml_files), "wrote_rows": 0}

    print(f"[loader] total parsed rules (pre-dedupe): {len(all_rules)}")

    pre_dedupe = len(all_rules)
    all_rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=batch_dedupe_mode)
    post_dedupe = len(all_rules)

    df = spark.createDataFrame(all_rules, schema=TABLE_SCHEMA)
    debug_display_batch(spark, df)

    unique_check_ids = df.select("check_id").distinct().count()
    distinct_pairs  = df.select("check_id", "run_config_name").distinct().count()

    if dry_run:
        display_section("DRY-RUN: FULL RULES PREVIEW")
        show_df(df.orderBy("table_name", "name"), n=1000, truncate=False)
        return {
            "mode": "dry_run",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "rules_pre_dedupe": pre_dedupe,
            "rules_post_dedupe": post_dedupe,
            "unique_check_ids": unique_check_ids,
            "distinct_rule_run_pairs": distinct_pairs,
            "target_table": delta_table_name,
            "wrote_rows": 0,
            "primary_key": primary_key,
        }

    overwrite_rules_into_delta(spark, df, delta_table_name, table_doc=None, primary_key=primary_key)
    wrote_rows = df.count()
    print(f"{Color.b}{Color.ivory}Finished writing rules to '{Color.r}{Color.b}{Color.i}{Color.sea_green}{delta_table_name}{Color.r}{Color.b}{Color.ivory}' (overwrite){Color.r}.")

    return {
        "mode": "overwrite",
        "config_path": output_config_path,
        "rules_files": len(yaml_files),
        "rules_pre_dedupe": pre_dedupe,
        "rules_post_dedupe": post_dedupe,
        "unique_check_ids": unique_check_ids,
        "distinct_rule_run_pairs": distinct_pairs,
        "target_table": delta_table_name,
        "wrote_rows": wrote_rows,
        "constraint": f"pk_{primary_key}",
        "primary_key": primary_key,
    }

def load_checks(
    dqx_cfg_yaml: str = "resources/dqx_config.yaml",
    created_by: str = "AdminUser",
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False,
    batch_dedupe_mode: str = "warn",
    table_doc: Optional[Dict[str, Any]] = None,
    apply_table_metadata: bool = False,
):
    return main(
        output_config_path=dqx_cfg_yaml,
        rules_dir=None,
        time_zone=time_zone,
        dry_run=dry_run,
        validate_only=validate_only,
        required_fields=None,
        batch_dedupe_mode=batch_dedupe_mode,
        table_doc=table_doc,
        created_by=created_by,
        apply_table_metadata=apply_table_metadata,
    )

# ---- run it ----
res = load_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    apply_table_metadata=True,
    # dry_run=True,
    # validate_only=True,
    batch_dedupe_mode="warn",
)
print(res)

