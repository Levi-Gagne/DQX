# src/dqx/info/dqx_structured_schema.py



##################################################################

# checks_config:
CHECKS_CONFIG_SCHEMA: List[Tuple[str, T.DataType, bool, str]] = [
    ("check_id",         T.StringType(), False, "PRIMARY KEY. Stable sha256 over canonical {table_name↓, filter, check.*}."),
    ("check_id_payload", T.StringType(), False, "Canonical JSON used to derive `check_id` (sorted keys, normalized values)."),
    ("table_name",       T.StringType(), False, "Target table FQN (`catalog.schema.table`). Lowercased in payload for stability."),
    ("name",        T.StringType(), False, "Human-readable rule name. Used in UI/diagnostics and joins."),
    ("criticality", T.StringType(), False, "Rule severity: `error|warn`."),
    ("check", T.StructType([
        T.StructField("function",        T.StringType(), False, {"comment": "DQX function to run"}),
        T.StructField("for_each_column", T.ArrayType(T.StringType()), True,  {"comment": "Optional list of columns"}),
        T.StructField("arguments",       T.MapType(T.StringType(), T.StringType()), True, {"comment": "Key/value args"}),
    ]), False, "Structured rule `{function, for_each_column?, arguments?}`; values stringified."),
    ("filter",          T.StringType(), True,  "Optional SQL predicate applied before evaluation (row-level)."),
    ("run_config_name", T.StringType(), False, "Execution group/tag. Not part of identity."),
    ("user_metadata",   T.MapType(T.StringType(), T.StringType()), True, "Free-form map<string,string>."),
    ("yaml_path",  T.StringType(),  False, "Absolute/volume path to the defining YAML doc (lineage)."),
    ("active",     T.BooleanType(), False, "If `false`, rule is ignored by runners."),
    ("created_by", T.StringType(),  False, "Audit: creator/principal that materialized the row."),
    ("created_at", T.StringType(),  False, "Audit: creation timestamp (cast to TIMESTAMP on write)."),
    ("updated_by", T.StringType(),  True,  "Audit: last updater (nullable)."),
    ("updated_at", T.StringType(),  True,  "Audit: last update timestamp (nullable; cast to TIMESTAMP on write)."),
]



########################################################################



# checks_log:
CHECKS_LOG_SCHEMA = T.StructType([
    T.StructField("log_id",                      T.StringType(),  False),  # PK (deterministic)
    # (no top-level check_id)
    T.StructField("table_name",                  T.StringType(),  False),
    T.StructField("run_config_name",             T.StringType(),  False),

    # _errors (array of issue structs; includes check_id inside each element)
    T.StructField("_errors", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False),
    T.StructField("_errors_fingerprint",         T.StringType(),  False),

    # _warnings (array of issue structs; includes check_id inside each element)
    T.StructField("_warnings", T.ArrayType(T.StructType([
        T.StructField("name",          T.StringType(), True),
        T.StructField("message",       T.StringType(), True),
        T.StructField("columns",       T.ArrayType(T.StringType()), True),
        T.StructField("filter",        T.StringType(), True),
        T.StructField("function",      T.StringType(), True),
        T.StructField("run_time",      T.TimestampType(), True),
        T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
        T.StructField("check_id",      T.StringType(), True),
    ])), False),
    T.StructField("_warnings_fingerprint",       T.StringType(),  False),

    T.StructField("row_snapshot", T.ArrayType(T.StructType([
        T.StructField("column",        T.StringType(), False),
        T.StructField("value",         T.StringType(), True),
    ])), False),
    T.StructField("row_snapshot_fingerprint",    T.StringType(),  False),

    T.StructField("created_by",                  T.StringType(),  False),
    T.StructField("created_at",                  T.TimestampType(), False),
    T.StructField("updated_by",                  T.StringType(),  True),
    T.StructField("updated_at",                  T.TimestampType(), True),
])



########################################################################



# summary_by_rule:
# Aggregated counts of row-level hits by table and rule for a given run_config.
# One row per (run_config_name, table_name, rule_name, severity).
# Calculations:
#   - rows_flagged: count of rows where this rule fired on the table
#   - table_total_rows: total rows scanned for that table (denominator)
#   - pct_of_table_rows: rows_flagged / table_total_rows (0.0 if denominator is 0)
CHECKS_LOG_SUMMARY_BY_TABLE_SCHEMA = T.StructType([
    T.StructField("run_config_name",   T.StringType(),  False),
    T.StructField("table_name",        T.StringType(),  False),
    T.StructField("rule_name",         T.StringType(),  False),
    T.StructField("severity",          T.StringType(),  False),  # 'error' | 'warning'
    T.StructField("rows_flagged",      T.LongType(),    False),
    T.StructField("table_total_rows",  T.LongType(),    True),
    T.StructField("pct_of_table_rows", T.DoubleType(),  False),
])



########################################################################



# summary_by_table:
# Per-table totals across all processed tables (per run_config) for this run.
# Calculations:
#   - table_total_rows: count(*)
#   - table_total_error_rows: count(rows with _errors non-empty)
#   - table_total_warning_rows: count(rows with _warnings non-empty)
#   - total_flagged_rows: count(rows with _errors or _warnings non-empty)
#   - distinct_rules_fired: count_distinct(issue.name across both severities)
CHECKS_LOG_SUMMARY_BY_TABLE_SCHEMA = T.StructType([
    T.StructField("run_config_name",          T.StringType(), False),
    T.StructField("table_name",               T.StringType(), False),
    T.StructField("table_total_rows",         T.LongType(),   False),
    T.StructField("table_total_error_rows",   T.LongType(),   False),
    T.StructField("table_total_warning_rows", T.LongType(),   False),
    T.StructField("total_flagged_rows",       T.LongType(),   False),
    T.StructField("distinct_rules_fired",     T.IntegerType(), False),
])



########################################################################

# Import for file below: 
from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED


# src/dqx/resources/dqx_functions_0_8_0.py

from __future__ import annotations
from typing import Dict

# DQX checks → expected argument types (row + dataset level)
EXPECTED: Dict[str, Dict[str, str]] = {
    # ---------- Row-level ----------
    "is_not_null": {"column": "str"},
    "is_not_empty": {"column": "str"},
    "is_not_null_and_not_empty": {"column": "str", "trim_strings": "bool"},
    "is_in_list": {"column": "str", "allowed": "list"},
    "is_not_null_and_is_in_list": {"column": "str", "allowed": "list"},
    "is_not_null_and_not_empty_array": {"column": "str"},

    "is_in_range": {
        "column": "str",
        "min_limit": "num",
        "max_limit": "num",
        "inclusive_min": "bool",
        "inclusive_max": "bool",
    },
    "is_not_in_range": {
        "column": "str",
        "min_limit": "num",
        "max_limit": "num",
        "inclusive_min": "bool",
        "inclusive_max": "bool",
    },
    "is_not_less_than": {"column": "str", "limit": "num"},
    "is_not_greater_than": {"column": "str", "limit": "num"},

    "is_valid_date": {"column": "str", "date_format": "str"},
    "is_valid_timestamp": {"column": "str", "timestamp_format": "str"},

    "is_not_in_future": {"column": "str", "offset": "num", "curr_timestamp": "str"},
    "is_not_in_near_future": {"column": "str", "offset": "num", "curr_timestamp": "str"},

    "is_older_than_n_days": {
        "column": "str",
        "days": "num",
        "curr_date": "str",
        "negate": "bool",
    },
    "is_older_than_col2_for_n_days": {
        "column1": "str",
        "column2": "str",
        "days": "num",
        "negate": "bool",
    },

    "regex_match": {"column": "str", "regex": "str", "negate": "bool"},

    "sql_expression": {
        "expression": "str",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "columns": "list",
    },

    "is_valid_ipv4_address": {"column": "str"},
    "is_ipv4_address_in_cidr": {"column": "str", "cidr_block": "str"},

    "is_data_fresh": {
        "column": "str",
        "max_age_minutes": "num",
        "base_timestamp": "str",
    },

    # ---------- Dataset-level ----------
    "is_unique": {"columns": "list", "nulls_distinct": "bool"},
    "is_aggr_not_greater_than": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_not_less_than": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_equal": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },
    "is_aggr_not_equal": {
        "column": "str",
        "limit": "num",
        "aggr_type": "str",
        "group_by": "list",
    },

    "foreign_key": {
        "columns": "list",
        "ref_columns": "list",
        "ref_df_name": "str",
        "ref_table": "str",
        "negate": "bool",
    },

    "sql_query": {
        "query": "str",
        "merge_columns": "list",
        "msg": "str",
        "name": "str",
        "negate": "bool",
        "condition_column": "str",
        "input_placeholder": "str",
        "row_filter": "str",
    },

    "compare_datasets": {
        "columns": "list",
        "ref_columns": "list",
        "exclude_columns": "list",
        "ref_df_name": "str",
        "ref_table": "str",
        "check_missing_records": "bool",
        "null_safe_row_matching": "bool",
        "null_safe_column_value_matching": "bool",
    },

    "is_data_fresh_per_time_window": {
        "column": "str",
        "window_minutes": "num",
        "min_records_per_window": "num",
        "lookback_windows": "num",
        "curr_timestamp": "str",
    },
}

__all__ = ["EXPECTED"]
