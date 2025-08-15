# Runbook — 02_run_dqx_checks (Run DQX Checks)

**Purpose**  
Execute Databricks Labs DQX rules from the config table, write a row‑level results log, and produce lightweight rollups for dashboards.

---

## At a glance
- **Notebook:** `02_run_dqx_checks`
- **Engine:** `databricks-labs-dqx==0.8.x`
- **Reads:** rules from `dqx_checks_config_table_name` (Delta)
- **Writes:**
  - **Row log:** `dqx_checks_log_table_name` (Delta, one row per flagged source row)
  - **Summary (by rule):** `dq_dev.dqx.checks_log_summary_by_rule` (append)
  - **Summary (by table):** `dq_dev.dqx.checks_log_summary_by_table` (overwrite)
- **Key behavior:** embeds `check_id` **inside each issue element** of `_errors`/`_warnings` (no top‑level `check_id` column). Deterministic `log_id` prevents accidental duplicates within a run.

---

## Prerequisites
- Unity Catalog schemas exist and you have permission to **read source tables** and **write** to the targets above.
- Rules have been loaded by **01_load_dqx_checks** into the checks config table.
- Cluster/SQL warehouse with Delta support. The notebook enables `spark.databricks.delta.schema.autoMerge.enabled=true` for nested struct evolution.

---

## Inputs & Config
YAML at `resources/dqx_config.yaml` (or your path) must define:
```yaml
dqx_checks_config_table_name: dq_dev.dqx.checks           # rules table (from loader)
dqx_checks_log_table_name:    dq_dev.dqx.checks_log       # results (row log)
run_config_name:                                          # named run configs (RCs)
  nightly:
    output_config:
      mode: overwrite                                     # overwrite|append
      options: {}                                         # any DataFrameWriter options
  ad_hoc:
    output_config:
      mode: append
      options: {}
```

**Rules selection per RC:** active rules where `run_config_name == <RC>` are loaded from the checks table.

---

## How to run
Minimal usage in a cell:
```python
# One‑time per cluster session
# %pip install databricks-labs-dqx==0.8.0
# dbutils.library.restartPython()

run_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    time_zone="America/Chicago",
    exclude_cols=None,            # optional: columns to exclude from row_snapshot
    coercion_mode="strict"        # "permissive" or "strict" argument coercion
)
```

**Parameters**
- `dqx_cfg_yaml` – path to the YAML above.
- `created_by` – audit field written to the row log.
- `time_zone` – for banners/printing only.
- `exclude_cols` – columns to omit from `row_snapshot` (e.g., large blobs).
- `coercion_mode` – how strictly to coerce stringified rule arguments (`strict` recommended).

---

## What it does (flow)
1. **Environment banner** and YAML load.  
2. **Ensure row‑log table exists** (creates empty Delta table with schema & comments if missing).  
3. For each **run config (RC)** in YAML:  
   a. **Load active rules** for that RC from the checks table; JIT‑coerce argument types; validate with `DQEngine`. Invalid rules are skipped (isolated).  
   b. **Group by table**, read each source table, and call `DQEngine.apply_checks_by_metadata`. If a batch fails, the runner tests rules individually and drops offenders.  
   c. **Project row hits** to the row‑log schema, computing:
      - `row_snapshot` = `[{"column","value"}...]` over non‑reserved columns (stringified)  
      - Fingerprints for `_errors`, `_warnings`, and `row_snapshot`  
      - Deterministic `log_id = sha256(table_name, run_config_name, row_snapshot_fp, _errors_fp, _warnings_fp)`  
   d. **Embed `check_id`** into each issue element (errors & warnings) by matching `(table, run_config_name, rule name, filter)`.  
   e. **Write row log** to `dqx_checks_log_table_name` using RC’s `output_config` (default `overwrite`).  
   f. **Summaries:** display per‑table rollups; build **by‑rule** counts (including zero‑hit rules) and append to `dq_dev.dqx.checks_log_summary_by_rule`.  
4. **Grand rollup (all RCs)** is written to `dq_dev.dqx.checks_log_summary_by_table` (overwrite).  
5. **Final banner** prints total rows written.

---

## Row‑Log schema (essential fields)
- `log_id` *(PK)* – deterministic hash; ensures idempotency within a run.  
- `table_name`, `run_config_name` – lineage.  
- `_errors`, `_warnings` – arrays of issue structs:  
  `{ name, message, columns[], filter, function, run_time, user_metadata, check_id }`  
- `_errors_fingerprint`, `_warnings_fingerprint` – order/column‑order insensitive digests.  
- `row_snapshot` – array of `{column, value}` (stringified).  
- `row_snapshot_fingerprint` – digest included in `log_id`.  
- `created_by`, `created_at`, `updated_by`, `updated_at` – audit.

> **Note:** There is **no** top‑level `check_id` column; IDs live **inside** each issue element.

---

## Operational tips
- **Write mode per RC**: control idempotency via YAML (`overwrite` recommended for scheduled runs; `append` for incremental scenarios).  
- **Schema evolution**: if upgrading from a version without `issue.check_id`, the runner sets `autoMerge=true` to evolve arrays of structs in place.  
- **Performance**: large tables can be slow—filter with rule‑level `filter` predicates in the checks table; consider running by table RCs.  
- **Dashboards**: the downstream views in your dashboard should explode `_errors`/`_warnings` and join by embedded `check_id` when needed.

---

## Troubleshooting
- **“no checks loaded”** for an RC → ensure checks table has `active=true` and exact `run_config_name` match.  
- **Validation failures** → the runner will isolate bad rules and continue; check notebook output for offending rules (JSON logged). Fix the rule in YAML and re‑load with the loader.  
- **Duplicate rows in append mode** → `log_id` is deterministic for the same row snapshot & issues; if inputs change (e.g., timestamps), duplicates are expected. Prefer `overwrite` for repeatable runs.  
- **Missing `check_id` in issues** → verify rule names/filters/table names in checks table match the issues; embedding derives IDs by `(table_key, run_config_name, rule name, filter)`.

---

## Verification queries
```sql
-- Row log rows by run config
SELECT run_config_name, COUNT(*) AS rows
FROM dq_dev.dqx.checks_log
GROUP BY run_config_name
ORDER BY rows DESC;

-- Any issue elements missing check_id?
SELECT SUM(size(filter(_errors,   x -> x.check_id IS NULL))) AS errors_missing_id,
       SUM(size(filter(_warnings, x -> x.check_id IS NULL))) AS warnings_missing_id
FROM dq_dev.dqx.checks_log;

-- Latest summaries
SELECT * FROM dq_dev.dqx.checks_log_summary_by_rule   ORDER BY run_config_name, rows_flagged DESC;
SELECT * FROM dq_dev.dqx.checks_log_summary_by_table  ORDER BY run_config_name, table_name;
```
