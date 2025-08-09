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


###############


# =========================================
# DQX: Load rules from config, run, log hits
# =========================================
from __future__ import annotations
import json
from typing import Dict, Any, List, Tuple

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

# ---------------------------
# 🔧 CONFIG (EDIT THESE THREE)
# ---------------------------
CHECKS_TABLE   = "dq_dev.dqx.checks_config"   # your config table with TABLE_SCHEMA you shared
RUN_CONFIG     = "default"                    # which run_config_name to pull
RESULTS_TABLE  = "dq_dev.dqx.checks_log"      # where we will write hits

WRITE_MODE = "overwrite"  # <- as requested

spark = SparkSession.builder.getOrCreate()

# =========================
# Results (log) table schema
# =========================
DQX_CHECKS_LOG_SCHEMA = T.StructType([
    T.StructField("result_id",       T.StringType(),  False),
    T.StructField("rule_id",         T.StringType(),  False),  # <- from checks_config.hash_id
    T.StructField("source_table",    T.StringType(),  False),
    T.StructField("run_config_name", T.StringType(),  False),
    T.StructField("severity",        T.StringType(),  False),  # "error" or "warning"
    T.StructField("name",            T.StringType(),  True),
    T.StructField("message",         T.StringType(),  True),
    T.StructField("columns",         T.ArrayType(T.StringType()), True),
    T.StructField("filter",          T.StringType(),  True),
    T.StructField("function",        T.StringType(),  True),
    T.StructField("run_time",        T.TimestampType(), True),
    T.StructField("user_metadata",   T.MapType(T.StringType(), T.StringType()), True),

    # row tracing
    T.StructField("row_fingerprint", T.StringType(),  True),   # md5 of row_json
    T.StructField("row_json",        T.StringType(),  True),   # full offending row as JSON

    # audit
    T.StructField("created_by",      T.StringType(),  False),
    T.StructField("created_at",      T.TimestampType(), False),
    T.StructField("updated_by",      T.StringType(),  True),
    T.StructField("updated_at",      T.TimestampType(), True),
])

# -----------------------
# Helpers: table + schema
# -----------------------
def ensure_log_table(full_name: str) -> None:
    if not spark.catalog.tableExists(full_name):
        cat, sch, _ = full_name.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA) \
            .write.format("delta").mode("overwrite").saveAsTable(full_name)

# -------------------------
# Helpers: rule rehydration
# -------------------------
def _maybe_json(s: str):
    if s is None:
        return None
    s = s.strip()
    if not s:
        return s
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return s
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    # try number
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return s

def _rehydrate_rules_from_config(checks_table: str, run_config: str) -> List[dict]:
    """
    Pull active rules for run_config, turn MAP<string,string> 'arguments' back into proper dicts.
    """
    df = (
        spark.read.table(checks_table)
        .filter((F.col("run_config_name") == run_config) & (F.col("active") == True))
        .select(
            "table_name", "name", "criticality", "run_config_name", "filter",
            F.col("check.function").alias("function"),
            F.col("check.for_each_column").alias("for_each_column"),
            F.col("check.arguments").alias("arguments"),  # map<string,string>
            "user_metadata",
            "hash_id",  # keep for later join
        )
    )

    rules: List[dict] = []
    for r in df.collect():
        args_map = r["arguments"] or {}
        py_args: Dict[str, Any] = {k: _maybe_json(v) for k, v in args_map.items()}

        check_block: Dict[str, Any] = {"function": r["function"]}
        if r["for_each_column"]:
            check_block["for_each_column"] = list(r["for_each_column"])
        if py_args:
            check_block["arguments"] = py_args

        rd = {
            "table_name":    r["table_name"],
            "name":          r["name"],
            "criticality":   r["criticality"],  # "error" | "warn"
            "run_config_name": r["run_config_name"],
            "check":         check_block,
        }
        if r["filter"]:
            rd["filter"] = r["filter"]
        if r["user_metadata"]:
            rd["user_metadata"] = dict(r["user_metadata"])
        # stash hash_id alongside for later (not part of DQX rule dict)
        rd["_hash_id"] = r["hash_id"]
        rules.append(rd)

    return rules

def _group_by_table(rules: List[dict]) -> Dict[str, List[dict]]:
    by_tbl: Dict[str, List[dict]] = {}
    for r in rules:
        t = r.get("table_name")
        if t:
            by_tbl.setdefault(t, []).append(r)
    return by_tbl

def _validate_rules(rules: List[dict]) -> None:
    status = DQEngine.validate_checks(rules)
    if getattr(status, "has_errors", False):
        # status.to_string() prints human readable errors
        raise ValueError(f"DQX validation failed:\n{status.to_string()}")

# ----------------------
# Helpers: make hit rows
# ----------------------
def _make_hits(
    annotated: DataFrame,
    source_table: str,
    run_config: str,
    created_by: str,
    src_cols: List[str]
) -> DataFrame:
    """
    From annotated DF (with _errors/_warnings or _error/_warning), build one row per violation
    and add row_fingerprint + row_json.
    """
    err_col = "_errors" if "_errors" in annotated.columns else "_error"
    wrn_col = "_warnings" if "_warnings" in annotated.columns else "_warning"

    row_struct = F.struct(*[F.col(c) for c in src_cols])
    base = annotated.select(
        F.to_json(row_struct).alias("row_json"),
        F.md5(F.to_json(row_struct)).alias("row_fingerprint"),
        F.col(err_col).alias("errs"),
        F.col(wrn_col).alias("warns"),
    )

    # explode only when arrays present
    errors = (
        base.where(F.size(F.col("errs")) > 0)
            .select("row_json", "row_fingerprint", F.explode(F.col("errs")).alias("r"))
            .select("row_json", "row_fingerprint", "r.*")
            .withColumn("severity", F.lit("error"))
    )
    warnings = (
        base.where(F.size(F.col("warns")) > 0)
            .select("row_json", "row_fingerprint", F.explode(F.col("warns")).alias("r"))
            .select("row_json", "row_fingerprint", "r.*")
            .withColumn("severity", F.lit("warning"))
    )

    out = errors.unionByName(warnings, allowMissingColumns=True)

    if out.rdd.isEmpty():
        return spark.createDataFrame([], DQX_CHECKS_LOG_SCHEMA)

    # attach static cols & audit
    cols_order = [f.name for f in DQX_CHECKS_LOG_SCHEMA.fields]
    out = (
        out
        .withColumn("result_id",       F.expr("uuid()"))
        .withColumn("source_table",    F.lit(source_table))
        .withColumn("run_config_name", F.lit(run_config))
        .withColumn("created_by",      F.lit(created_by))
        .withColumn("created_at",      F.current_timestamp())
        .withColumn("updated_by",      F.lit(None).cast(T.StringType()))
        .withColumn("updated_at",      F.lit(None).cast(T.TimestampType()))
        .select(*cols_order)  # will fill rule_id later via join
    )
    return out

# ---------------
# Main run logic
# ---------------
engine = DQEngine(WorkspaceClient())
ensure_log_table(RESULTS_TABLE)

# who is writing (for audit)
created_by = spark.sql("select current_user() as u").first()["u"]

# load + validate
rules = _rehydrate_rules_from_config(CHECKS_TABLE, RUN_CONFIG)
if not rules:
    raise RuntimeError(f"No active checks found in {CHECKS_TABLE} for run_config_name='{RUN_CONFIG}'.")

_validate_rules(rules)

by_table = _group_by_table(rules)
print(f"Loaded {len(rules)} rules across {len(by_table)} table(s): {list(by_table.keys())}")

# lookup for rule_id (hash_id) + function/args for summaries
lk = (
    spark.read.table(CHECKS_TABLE)
    .filter((F.col("run_config_name") == RUN_CONFIG) & (F.col("active") == True))
    .select(
        F.col("table_name").alias("lk_table"),
        F.col("run_config_name").alias("lk_rc"),
        F.col("name").alias("lk_name"),
        F.col("hash_id").alias("rule_id"),
        F.col("check.function").alias("lk_function"),
        F.col("check.arguments").alias("lk_args"),
    )
    .dropDuplicates()
)
lk = F.broadcast(lk)

all_hits: List[DataFrame] = []
total_err = total_warn = 0

for tbl, tbl_rules in by_table.items():
    print(f"\n=== Applying {len(tbl_rules)} rule(s) to {tbl} ===")
    src = spark.read.table(tbl)
    src_cols = src.columns
    rows_scanned = src.count()  # ok for your scale; comment out if heavy

    annotated = engine.apply_checks_by_metadata(src, tbl_rules)
    hits = _make_hits(annotated, tbl, RUN_CONFIG, created_by, src_cols)

    if hits.rdd.isEmpty():
        print(f"  Rows scanned: {rows_scanned:,}")
        print( "  Violations  : total=0 | error=0 | warning=0")
        continue

    # join to get rule_id from config using (table, rc, name)
    hits = (
        hits.alias("h").join(
            lk.alias("lk"),
            (F.col("h.source_table") == F.col("lk.lk_table")) &
            (F.col("h.run_config_name") == F.col("lk.lk_rc")) &
            (F.col("h.name") == F.col("lk.lk_name")),
            "left"
        )
        .drop("lk_table","lk_rc","lk_name")
    )

    # fill function from lookup when missing; optional but helpful
    hits = hits.withColumn("function", F.coalesce(F.col("function"), F.col("lk_function")))

    # drop hits that couldn't find rule_id (name mismatch etc.)
    missing = hits.filter(F.col("rule_id").isNull()).count()
    if missing:
        print(f"  Warning: {missing} hit(s) missing rule_id (name/run_config mismatch?) — dropping them")
    hits = hits.filter(F.col("rule_id").isNotNull())

    if hits.rdd.isEmpty():
        print(f"  Rows scanned: {rows_scanned:,}")
        print( "  Violations  : total=0 | error=0 | warning=0")
        continue

    # per-table summary
    v_total = hits.count()
    v_err   = hits.filter(F.col("severity") == "error").count()
    v_warn  = hits.filter(F.col("severity") == "warning").count()
    v_rows  = hits.select("row_fingerprint").distinct().count()

    print(f"  Rows scanned: {rows_scanned:,}")
    print(f"  Rules ran   : {len(tbl_rules)}")
    print(f"  Violations  : total={v_total:,} | error={v_err:,} | warning={v_warn:,}")
    print(f"  Rows impacted (distinct row_fingerprint): {v_rows:,}")

    # per-rule summary for this table
    per_rule = (
        hits.groupBy("name","function")
            .agg(
                F.count("*").alias("violations"),
                F.countDistinct("row_fingerprint").alias("rows_impacted"),
            )
            .orderBy(F.desc("violations"))
    )
    try:
        display(per_rule)
    except NameError:
        per_rule.show(truncate=False)

    all_hits.append(hits.select([f.name for f in DQX_CHECKS_LOG_SCHEMA.fields]))
    total_err += v_err
    total_warn += v_warn

# -------------------
# Write + global summ
# -------------------
if not all_hits:
    print("\nNo violations to write.")
else:
    out = all_hits[0]
    for df in all_hits[1:]:
        out = out.unionByName(df, allowMissingColumns=True)

    # global per-rule summary
    global_per_rule = (
        out.groupBy("source_table","name","function")
           .agg(
               F.count("*").alias("violations"),
               F.countDistinct("row_fingerprint").alias("rows_impacted"),
           )
           .orderBy(F.desc("violations"))
    )
    print("\n=== GLOBAL SUMMARY ===")
    print(f"Total violations -> error={total_err:,}, warning={total_warn:,}, total={(total_err+total_warn):,}")
    try:
        display(global_per_rule)
    except NameError:
        global_per_rule.show(truncate=False)

    # persist (OVERWRITE)
    written = out.count()
    out.write.format("delta").mode(WRITE_MODE).saveAsTable(RESULTS_TABLE)
    print(f"\nWrote {written:,} rows to {RESULTS_TABLE} (mode={WRITE_MODE})")