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