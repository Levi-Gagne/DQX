# src/dqx/info/dqx_structured_schema.py

from resources.dqx_functions_0_8_0 import EXPECTED as _EXPECTED

##################################################################

# checks_config:
DQX_CHECKS_CONFIG_COLUMN_SPECS: List[Tuple[str, T.DataType, bool, str]] = [
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
ROW_LOG_SCHEMA = T.StructType([
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