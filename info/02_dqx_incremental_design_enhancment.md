# CLA DQX — Incremental Runner Plan (CDF + Watermark)

Goal: move from “scan everything, every day” to incremental without lying to ourselves. Keep it practical and honest about constraints.

---

## 0) TL;DR

- **Row‑level checks** (nulls, ranges, regex, sql_expression on a row) can run **incrementally** if we can isolate changed rows.
- **Dataset‑level checks** (is_unique, foreign_key, windowed counts) need **history awareness** → we keep a **key index** and update it incrementally.
- **Two modes** (use whichever the table supports):
  1) **CDF Mode** (preferred): Delta **Change Data Feed** by table version.
  2) **Watermark Mode**: “last seen” value on a **monotonic column** (id or event_ts). If we don’t have one, it’s snapshot‑only until we introduce one.
- **Identity & dedupe:** `rule_id` = logic hash of functional semantics; `hit_id = sha256(rule_id || '|' || violation_key)` so re‑runs don’t double‑write.
- **Delta Sharing note:** treat shared tables as **snapshot only** (no CDF). Use Watermark Mode or snapshot‑diff + our own state in our catalog.

---

## 1) What blocks incrementality today

We don’t consistently know a safe **watermark column** (id or event time) for many sources. Without CDF or a watermark, “incremental” is guesswork. Also, some rules (e.g., **is_unique**) require comparing **new** rows against **all historical** keys — you can’t do that correctly without a state table or a full scan.

---

## 2) Rule identity & violation identity (what we write)

- **Rule identity (`rule_id`)**: SHA‑256 over canonical `{ table_name↓, filter(normalized), check{ function↓, for_each_column(sorted)?, arguments(stringified, keys sorted)? } }`.  
  Excludes `name`, `criticality`, `run_config_name`, `user_metadata`.
- **Violation identity (`hit_id`)**: `sha256(rule_id || '|' || violation_key)` where `violation_key` is stable per offending row (e.g., business PK JSON or a row checksum if no PK).

Why: `rule_id` lets us track a rule across renames; `hit_id` makes writes idempotent.

---

## 3) Modes

### 3.1 CDF Mode (Delta Change Data Feed)

**Use when:** the table is Delta and `delta.enableChangeDataFeed = true`.

**How it works**  
- We track the last processed **table version** per `(table, rule_id)` in `dq_dev.dqx_progress`.  
- On each run, we read only changes `startingVersion = last_version + 1`.  
- Row‑level checks run on this diff.  
- Dataset‑level checks update a **key index** incrementally (see §4).

```sql
-- One-time per table
ALTER TABLE <catalog.schema.table> SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

```python
# Pseudocode (Spark)
changes = (spark.read.format("delta")
           .option("readChangeFeed","true")
           .option("startingVersion", last_version + 1)
           .table(table_fqn)
           # Use postimage for latest values; handle deletes separately
           .where("_change_type IN ('insert','update_postimage','delete')"))

# Row-level: evaluate only 'insert' and 'update_postimage'
delta_rows = changes.where("_change_type IN ('insert','update_postimage')")

# Dataset-level: feed key-index with (+1/-1) for inserts/deletes; updates = -1 old +1 new if you read preimage+postimage.
```

**Pros:** true incremental without a business watermark, handles updates/deletes.  
**Cons:** must own the table (or have provider enable CDF).

### 3.2 Watermark Mode (Snapshot + Max Value)

**Use when:** no CDF, but we have a **monotonic** column (`event_ts`, `ingest_id`, etc.).

**How it works**  
- Track `last_value` per `(table, watermark_column)` in `dqx_progress`.  
- Next run = `WHERE watermark_column > last_value`.  
- Row‑level checks work fine.  
- Dataset‑level checks still need the **key index** to detect conflicts against history.

```yaml
# Example config mapping (table → watermark column)
watermarks:
  dq_prd.monitoring.job_run_audit: start_time
  dq_prd.txn.fact_orders: ingest_ts
```

**Pros:** simple, no table property required.  
**Cons:** requires a trustworthy monotonic column; won’t catch late arrivals or updates unless the watermark updates too.

> If we have neither CDF nor a monotonic column, we are **snapshot‑only**. Options: add `ingest_ts` at ingestion, or do a snapshot‑diff against our last saved key list (expensive).

---

## 4) Dataset‑level checks need a **Key Index**

**Why:** `is_unique([k…])`, `foreign_key`, grouped `sql_query` rules must compare new rows to history.

**Table:** `dq_dev.dqx_key_index`

| col              | type      | notes |
|------------------|-----------|------|
| table_name       | STRING    | FQN |
| rule_id          | STRING    | logic hash |
| key_hash         | STRING    | sha256(JSON of key tuple) |
| key_json         | STRING    | stable JSON of key tuple (for debugging) |
| count            | BIGINT    | refcount of seen rows for this key |
| first_seen       | TIMESTAMP | when inserted |
| last_seen        | TIMESTAMP | last touch |

**Updates**  
- **CDF Mode:**  
  - `insert`/`update_postimage` → `count += 1` for that key.  
  - `delete`/`update_preimage` → `count -= 1`.  
- **Watermark Mode:** treat all new rows as `+1`. (We can’t see deletes unless publisher rewrites history or watermark captures updates.)

**Detecting violations**  
- For `is_unique(keys)`: violation exists when `count > 1`.  
- For `foreign_key`: maintain a **separate dimension index** (or compute misses by anti‑join on the current dimension snapshot).

---

## 5) Control: `dq_dev.dqx_progress`

| col          | type      | semantics |
|--------------|-----------|-----------|
| table_name   | STRING    | FQN |
| rule_id      | STRING    | logic hash |
| mode         | STRING    | `cdf` \| `watermark` \| `snapshot` |
| watermark_col| STRING    | nullable; used in watermark mode |
| last_version | BIGINT    | nullable; CDF mode |
| last_value   | STRING    | nullable; watermark mode (store as string) |
| processed_at | TIMESTAMP | audit |

We keep one row per `(table_name, rule_id)` and update it after a successful write.

---

## 6) Runner outline (what the notebook/job does)

```python
for group in rules.group_by_table():
    table = group.table_name
    mode, spec = choose_mode(table)  # cdf if enabled else watermark if configured else snapshot

    if mode == "cdf":
        changes = read_cdf_since_version(table, spec.last_version)  # postimage rows
        df_rows = changes.where("_change_type IN ('insert','update_postimage')")
        run_rowlevel_rules(df_rows, group.row_rules)                 # write violations w/ hit_id
        update_key_index_from_cdf(changes, group.dataset_rules)      # +/- counts
        write_dataset_violations_from_index(table, group.dataset_rules)
        update_progress_version(table, group.rule_ids, new_table_version)

    elif mode == "watermark":
        df_rows = spark.table(table).where(f"{spec.col} > '{spec.last_value}'")
        run_rowlevel_rules(df_rows, group.row_rules)
        update_key_index_from_rows(df_rows, group.dataset_rules)
        write_dataset_violations_from_index(table, group.dataset_rules)
        update_progress_value(table, group.rule_ids, df_rows.agg({"spec.col":"max"}))

    else:  # snapshot (no incremental possible)
        df_all = spark.table(table)
        run_rowlevel_rules(df_all, group.row_rules)
        rebuild_key_index(df_all, group.dataset_rules)  # heavy
        write_dataset_violations_from_index(table, group.dataset_rules)
```

**Writes are idempotent** via `hit_id`. We `MERGE` into the violations table on `(hit_id)` to avoid dup rows on retries.

---

## 7) Delta Sharing note (consumer side)

- Assume **snapshot‑only** access. CDF and table versions generally aren’t exposed through Delta Sharing.  
- We can still do **Watermark Mode** if the shared table has a trustworthy column (`event_ts`, `id`, etc.).  
- If not, we keep a **consumer‑side snapshot** of keys and anti‑join each run to find new keys (costly but workable on small key sets).  
- All **state tables** (`dqx_progress`, `dqx_key_index`, violations) live in **our** catalog; the shared source remains read‑only.

---

## 8) Performance notes (don’t burn the cluster)

- **Row‑level null checks:** prefer `for_each_column` over dataset scans; pair with filters to shrink I/O.  
- **Key Index:** store **hash + JSON**; hash for joins, JSON for human debugging. Don’t explode it with unused columns.  
- **Updates:** in CDF, use **postimage** for “new value” counts and subtract on preimage when available.  
- **Late data:** watermark mode won’t see backfills unless the watermark column moves forward with them (event_time vs load_time matters).

---

## 9) Migration path from “full scan”

1) Add/standardize a watermark where missing (prefer `ingest_ts` or a durable `event_ts`).  
2) Enable **CDF** on owner‑managed Delta tables.  
3) Create `dqx_progress` + `dqx_key_index`.  
4) Compute `rule_id` and `hit_id` in the runner (we already compute `rule_id` in the loader).  
5) Switch row‑level rules first. Add dataset‑level once the key index has warmed up.  
6) Keep a fallback job for “snapshot‑only” tables until they’re fixed.

---

## 10) Minimal DDL

```sql
CREATE TABLE IF NOT EXISTS dq_dev.dqx_progress (
  table_name   STRING,
  rule_id      STRING,
  mode         STRING,
  watermark_col STRING,
  last_version BIGINT,
  last_value   STRING,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS dq_dev.dqx_key_index (
  table_name STRING,
  rule_id    STRING,
  key_hash   STRING,
  key_json   STRING,
  count      BIGINT,
  first_seen TIMESTAMP,
  last_seen  TIMESTAMP
) USING DELTA;
```

---

## 11) What we will **not** pretend

- Without CDF **or** a real watermark column, incrementality is marketing. We either store a prior snapshot of keys and diff (expensive) or we run full scans. Pick one and be explicit in the run config.

---