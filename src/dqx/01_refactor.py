#######################################################
# src/dqx/resources/dqx_config.yaml
#######################################################

dqx_config:
  local_timezone: America/Chicago
  processing_timezone: UTC
  apply_table_metadata: false
  variables:            # optional runtime substitution variables
    env: dev            # used by dq_{env} → dq_dev

# ──────────────────────────────────────────────────────────────────────────────
# 01_load_dqx_checks
# ──────────────────────────────────────────────────────────────────────────────
dqx_load_checks:
  checks_config_source:
    storage_type: repo            # repo | volume
    source_format: yaml           # yaml | json | python
    source_path: resources/dqx_checks_config_load
    allowed_criticality: [error, warn, warning]
    required_fields: [table_name, name, criticality, run_config_name, check]

  checks_config_table:
    target_table: dq_{env}.dqx.checks_config
    table_format: delta
    write_mode: overwrite
    primary_key: check_id
    partition_by: []              # (none)
    table_comment: |
      ## DQX Checks Configuration
      - One row per **unique canonical rule** generated from YAML (source of truth).
      - **Primary key**: `check_id` (sha256 of canonical payload). Uniqueness is enforced by the loader and a runtime assertion.
      - Rebuilt by the loader (typically **overwrite** semantics); manual edits will be lost.
      - Used by runners to resolve rules per `run_config_name` and by logs to map back to rule identity.
      - `check_id_payload` preserves the exact canonical JSON used to compute `check_id` for reproducibility.
      - `run_config_name` is a **routing tag**, not part of identity.
      - Only rows with `active=true` are executed.
    columns:
      check_id:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}."
      check_id_payload:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Canonical JSON used to derive `check_id` (sorted keys, normalized values)."
      table_name:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability."
      name:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Human-readable rule name. Used in UI/diagnostics and name-based joins when enriching logs."
      criticality:
        datatype: string
        nullable: false
        allowed_values: [error, warn, warning]
        comment: "Rule severity: `warn|warning|error`. Reporting normalizes warn/warning → `warning`."
      check:
        datatype: struct<function:string,for_each_column:array<string>,arguments:map<string,string>>
        nullable: false
        allowed_values: []
        comment: "Structured rule: `{function, for_each_column?, arguments?}`; argument values stringified."
      filter:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "Optional SQL predicate applied before evaluation (row-level). Normalized in payload."
      run_config_name:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Execution group/tag. Drives which runs pick up this rule; **not** part of identity."
      user_metadata:
        datatype: map<string,string>
        nullable: true
        allowed_values: []
        comment: "Free-form `map<string,string>` carried through to issues for traceability."
      yaml_path:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Absolute/volume path to the defining YAML doc (lineage)."
      active:
        datatype: boolean
        nullable: false
        allowed_values: [true, false]
        comment: "If `false`, rule is ignored by runners."
      created_by:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Audit: creator/principal that materialized the row."
      created_at:
        datatype: timestamp
        nullable: false
        allowed_values: []
        comment: "Audit: creation timestamp (cast to TIMESTAMP on write)."
      updated_by:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "Audit: last updater (nullable)."
      updated_at:
        datatype: timestamp
        nullable: true
        allowed_values: []
        comment: "Audit: last update timestamp (nullable; cast to TIMESTAMP on write)."

# ──────────────────────────────────────────────────────────────────────────────
# 02_run_dqx_checks
# ──────────────────────────────────────────────────────────────────────────────
dqx_run_checks:
  checks_log_table:
    target_table: &checks_log_table dq_{env}.dqx.checks_log
    target_format: delta
    write_mode: overwrite
    primary_key: log_id
    partition_by: []
    table_comment: |
      ## DQX Row-Level Check Results Log
      - One row per **flagged source row** (error or warning) emitted by DQX for a given run.
      - **Primary key**: `log_id` = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp). **No duplicates allowed** within a run; duplicates indicate a repeated write or misconfigured mode.
      - `_errors/_warnings` capture issue structs; corresponding fingerprints are deterministic and order-insensitive.
      - `row_snapshot` captures the offending row’s non-reserved columns (values stringified) at evaluation time.
      - `check_id` lists originating rule IDs from the config table; may be empty if name-based mapping was not possible.
      - Writers should ensure idempotency (e.g., overwrite mode or dedupe by `log_id` when appending).
    columns:
      log_id:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "PRIMARY KEY. Deterministic sha256 over (table_name, run_config_name, row_snapshot_fingerprint, _errors_fingerprint, _warnings_fingerprint)."
      check_id:
        datatype: array<string>
        nullable: true
        allowed_values: []
        comment: "Array of originating config `check_id`s that fired for this row (may be empty if unmapped)."
      table_name:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Source table FQN (`catalog.schema.table`) where the row was evaluated."
      run_config_name:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Run configuration under which the checks executed."
      _errors:
        datatype: array<struct<name:string,message:string,columns:array<string>,filter:string,function:string,run_time:string,user_metadata:map<string,string>>>
        nullable: true
        allowed_values: []
        comment: "Array<struct> of error issues."
      _errors_fingerprint:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "Deterministic digest of normalized `_errors` (order/column-order insensitive)."
      _warnings:
        datatype: array<struct<name:string,message:string,columns:array<string>,filter:string,function:string,run_time:string,user_metadata:map<string,string>>>
        nullable: true
        allowed_values: []
        comment: "Array<struct> of warning issues."
      _warnings_fingerprint:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "Deterministic digest of normalized `_warnings`."
      row_snapshot:
        datatype: array<struct<column:string,value:string>>
        nullable: true
        allowed_values: []
        comment: "Array<struct{column:string, value:string}> of non-reserved columns for the flagged row (stringified)."
      row_snapshot_fingerprint:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "sha256(JSON(row_snapshot)) used in `log_id` and de-duplication."
      created_by:
        datatype: string
        nullable: false
        allowed_values: []
        comment: "Audit: writer identity for this record."
      created_at:
        datatype: timestamp
        nullable: false
        allowed_values: []
        comment: "Audit: creation timestamp (UTC)."
      updated_by:
        datatype: string
        nullable: true
        allowed_values: []
        comment: "Audit: last updater (nullable)."
      updated_at:
        datatype: timestamp
        nullable: true
        allowed_values: []
        comment: "Audit: last update timestamp (UTC, nullable)."

  checks_log_summary_by_rule_table:
    target_table: dq_{env}.dqx.checks_log_summary_by_rule
    target_format: delta
    write_mode: overwrite
    primary_key:
    partition_by: []

  checks_log_summary_by_table_table:
    target_table: dq_{env}.dqx.checks_log_summary_by_table
    target_format: delta
    write_mode: overwrite
    primary_key:
    partition_by: []
    created_by_default: AdminUser

# ──────────────────────────────────────────────────────────────────────────────
# Run configs — show all options; leave values empty unless you want to override.
# Anchors/aliases used to avoid repeating the checks_log_table target.
# ──────────────────────────────────────────────────────────────────────────────
run_config_name:
  default:
    output_config:
      location: *checks_log_table     # → dq_{env}.dqx.checks_log
      mode:
      format:
      options: {}
    quarantine_config:
      location:
      mode:
      format:
      options: {}
  generated_check:
    output_config:
      location: dq_{env}.dqx.generated_checks_log
      mode:
      format:
      options: {}
    quarantine_config:
      location: dq_{env}.dqx.generated_checks_quarantine
      mode:
      format:
      options: {}
      

#######


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



#######



# src/dqx/utils/documentation.py

from __future__ import annotations

import json
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from utils.display import show_df, display_section
from utils.config import ProjectConfig

__all__ = [
    "_materialize_table_doc",
    "_q_fqn",
    "_esc_comment",
    "preview_table_documentation",
    "apply_table_documentation",
    "doc_from_config",
]


def _materialize_table_doc(doc_template: Dict[str, Any], table_fqn: str) -> Dict[str, Any]:
    """Fill the table FQN into the doc template and expand placeholders."""
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
            spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

    # Column comments (best-effort)
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
            try:
                spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{_esc_comment(comment)}'")
            except Exception as e2:
                print(f"[meta] Skipped column comment for {table_fqn}.{col_name}: {e2}")


# Convenience: fetch and materialize table doc directly from YAML config
def doc_from_config(cfg: ProjectConfig, table_section: str, table_fqn: str) -> Optional[Dict[str, Any]]:
    """
    table_section: 'checks_config_table' | 'checks_log_table' | ...
    """
    tpl = cfg.table_doc(table_section)
    if not tpl:
        return None
    return _materialize_table_doc(tpl, table_fqn)
    
    
    
    
#######




