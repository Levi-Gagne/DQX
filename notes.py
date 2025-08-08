from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import functions as F
import hashlib, json

engine = DQEngine(WorkspaceClient())

TABLE = "dq_prd.monitoring.job_run_audit"
KEY_COLS = ["run_id"]
RUN_CONFIG = "default"

# --- your two rules (or load from YAML) ---
rules = [
    {
        "table_name": TABLE,
        "name": "run_id_is_not_null",
        "criticality": "error",
        "run_config_name": "default",
        "check": {"function": "is_not_null", "arguments": {"column": "run_id"}},
    },
    {
        "table_name": TABLE,
        "name": "failed_state_is_terminated",
        "criticality": "error",
        "run_config_name": "default",
        "filter": "result_state = 'FAILED'",
        "check": {"function": "sql_expression", "arguments": {"expression": "state = 'TERMINATED'"}},
    },
]

def rule_logic_hash(rule: dict) -> str:
    payload = {
        "table_name": str(rule["table_name"]).lower(),
        "check": {
            "function": str(rule["check"]["function"]).lower(),
            "arguments": rule["check"].get("arguments") or {},
            "for_each_column": rule["check"].get("for_each_column"),
        },
        "filter": rule.get("filter"),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

rule_rows = [(r["name"], rule_logic_hash(r), r["criticality"]) for r in rules]
rule_map = spark.createDataFrame(rule_rows, ["name", "rule_id", "criticality"])

def fp_expr(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)

df = spark.table(TABLE)

# Build a per-row JSON snapshot once
row_struct = F.struct(*[F.col(c).alias(c) for c in df.columns])
row_json   = F.to_json(row_struct)

annotated = engine.apply_checks_by_metadata(df, rules)  # adds _errors/_warnings
base = (annotated
        .withColumn("_fp", fp_expr(KEY_COLS))
        .withColumn("_row_snapshot_json", row_json))

errors = (base
    .select(*KEY_COLS, "_fp", "_row_snapshot_json", F.explode_outer(F.col("_errors")).alias("hit"))
    .withColumn("severity", F.lit("error")))

warnings = (base
    .select(*KEY_COLS, "_fp", "_row_snapshot_json", F.explode_outer(F.col("_warnings")).alias("hit"))
    .withColumn("severity", F.lit("warning")))

hits = (errors.unionByName(warnings, allowMissingColumns=True)
    .filter(F.col("hit").isNotNull())
    .select(*KEY_COLS, "_fp", "severity", "hit.*", F.col("_row_snapshot_json").alias("row_snapshot_json"))
    .join(rule_map, on="name", how="left")
    .withColumn("result_id", F.expr("uuid()"))
    .withColumn("source_table", F.lit(TABLE))
    .withColumn("run_config_name", F.lit(RUN_CONFIG))
    .withColumn("created_at", F.current_timestamp())
)

display(hits)

# Optional: summary for the dashboard
display(hits.groupBy("source_table", "rule_id", "name", "severity").count().orderBy(F.desc("count")))