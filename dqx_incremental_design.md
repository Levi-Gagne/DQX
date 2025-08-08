# DQX @ CLA — Incremental Runner Design, Keys, and CDF Playbook

## 0) TL;DR

- **Identity:** each rule has a **logic hash** (`logic_hash`) that ignores labels and run configs but captures functional semantics. We store it as both `logic_hash` and `hash_id` in the rules table, and inject it as `user_metadata.rule_id` so results carry it.
- **Results:** each violation gets a **hit id** (`hit_id = sha256(rule_id || '|' || violation_key)`), where `violation_key` is a stable, rule-specific fingerprint (row identity or key tuple).
- **Incremental by default:** enable **Delta Change Data Feed (CDF)** per table. Read only changed rows since the last processed **table version**, stored in a tiny control table `dqx_progress`.
- **Parallelism:** same notebook, **multiple tasks**, each with a **job cluster** and different inputs (e.g., `rules_dir` shards). This is how you scale **horizontally** without intra-cluster fighting.
- **Dataset-level uniqueness:** keep a small **state table** of key counts so you update only changed keys (optional v2).

---

## 1) Why this design

- You avoid O(all data × all rules) every run.
- You write only **new** violations; reruns are idempotent via `hit_id`.
- You can scale out by sharding tables or rule folders across tasks.

---

## 2) Identity and keys

### 2.1 Rule identity (already implemented in your loader)
- `logic_hash` = SHA-256 over a **canonical** payload:
  - **Include:** `table_name` (lowercased), `check.function` (lowercased), normalized `check.arguments`, normalized `check.for_each_column` (sorted), normalized `filter` (whitespace collapsed).
  - **Exclude:** `name`, `run_config_name`, `user_metadata`, `criticality`.
- Persist:
  - `rules.hash_id = logic_hash`
  - `rules.logic_hash = logic_hash`
  - `rules.user_metadata.rule_id = logic_hash` (so results carry it directly)

### 2.2 Violation identity (results)
- `rule_id` = `logic_hash` from rules table.
- `violation_key` = deterministic string unique per violating row. Example: concat primary key values or a stable row checksum.
- `hit_id` = `sha256(rule_id || '|' || violation_key)` — prevents duplicates on reruns.

---

## 3) Delta CDF incremental reads

### 3.1 Prereqs
- Table must be Delta.
- Enable CDF:
```sql
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
- Ensure `dqx_progress` exists to track last processed version per table/rule:
```sql
CREATE TABLE IF NOT EXISTS dq_dev.dqx_progress (
    table_name STRING,
    rule_id STRING,
    last_version BIGINT,
    processed_at TIMESTAMP
) USING DELTA;
```

### 3.2 Example: Reading changes since last version
```python
from delta.tables import DeltaTable

def read_cdf_incremental(spark, full_table_name, last_version):
    return (spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", last_version + 1)
            .table(full_table_name))
```

---

## 4) Parallelizing in Databricks Jobs

- Use **multi-task jobs**.
- Each task:
  - Same notebook.
  - Different `rules_dir` or config param passed as a job parameter.
  - Targets a smaller subset of rules/tables.
- **Cluster strategy:** to scale CPU/memory, run each task on its **own job cluster**. Running multiple tasks on the same cluster won't increase the total memory available; they'll just compete.

---

## 5) Suggested `dqx_progress` schema

| Column         | Type      | Purpose |
|----------------|-----------|---------|
| table_name     | STRING    | Fully qualified table name |
| rule_id        | STRING    | Logic hash from rules table |
| last_version   | BIGINT    | Last processed Delta table version |
| processed_at   | TIMESTAMP | When the version was processed |

---

## 6) Incremental loop outline

```python
def process_table_incrementally(spark, table_name, rule_id, last_version):
    df_changes = read_cdf_incremental(spark, table_name, last_version)
    # Apply your DQX rules here to df_changes instead of full table
    # ...
    new_version = DeltaTable.forName(spark, table_name).history().selectExpr("max(version)").first()[0]
    update_progress(spark, table_name, rule_id, new_version)

def update_progress(spark, table_name, rule_id, version):
    spark.sql(f'''
        MERGE INTO dq_dev.dqx_progress AS tgt
        USING (SELECT '{table_name}' AS table_name, '{rule_id}' AS rule_id, {version} AS last_version, current_timestamp() AS processed_at) AS src
        ON tgt.table_name = src.table_name AND tgt.rule_id = src.rule_id
        WHEN MATCHED THEN UPDATE SET last_version = src.last_version, processed_at = src.processed_at
        WHEN NOT MATCHED THEN INSERT *
    ''')
```

---

## 7) Notes

- If no natural PK exists, consider a **stable row checksum** of all columns as `violation_key`.
- For non-Delta sources, CDF is not available — you’d need other strategies (e.g., snapshot diffing).
- Keep your violation table **append-only**; rely on `hit_id` to dedupe.
