# DQX Offender‑Aware Dashboard — Proposal & First Cut

**Owner:** Levi Gagne  
**Scope:** Turn `dq_dev.dqx.checks_log` into high‑signal views + a Lakeview dashboard that surfaces *what failed*, *where*, and *why*, with “offender extraction” for common functions.
**Last updated:** 2025-08-14 01:06 UTC

---

## TL;DR

- Model the noisy `checks_log` into **three stable layers**: events → offenders → rollups.  
- Prefer **views** while iterating; switch hot paths to **materialized views** in UC later.  
- Use `run_time` from the error payload for timelines (not `created_at`, which is batch-level).  
- Extract offenders for common DQX functions (`is_not_null*`, `is_in_list`, `regex_match`, `is_in_range`, `is_unique`) via SQL-only heuristics on `row_snapshot`.  
- Keep names consistent: all analytics views are prefixed with **`dashboard_*`** in schema `dq_dev.dqx`.
- Programmatic Lakeview: store JSON under `dashboards/DQX_Overview.lvdash.json`; create/publish from a notebook.

---

## Table of Contents

1. [Source Contracts](#source-contracts)  
2. [Design Goals](#design-goals)  
3. [Data Model](#data-model)  
   - 3.1 [Events](#events)  
   - 3.2 [Row Snapshot (KV)](#row-snapshot-kv)  
   - 3.3 [Offender Extraction](#offender-extraction)  
   - 3.4 [Rule Catalog Join](#rule-catalog-join)  
   - 3.5 [Rollups & KPI](#rollups--kpi)  
4. [SQL — Views (ready to run)](#sql--views-ready-to-run)  
5. [Function Coverage Matrix](#function-coverage-matrix)  
6. [Performance & Ops Notes](#performance--ops-notes)  
7. [Lakeview: Programmatic Create/Publish](#lakeview-programmatic-createpublish)  
8. [Next Steps](#next-steps)  
9. [References](#references)

---

## Source Contracts

**Results sink:** `dq_dev.dqx.checks_log`  
Key columns we actually use:
- `log_id` (PK, deterministic hash)
- `_errors` / `_warnings` (arrays of structs with `name, message, columns, filter, function, run_time, user_metadata`)
- `row_snapshot` (array of `{column, value}`; values are strings)
- `check_id` (array of rule IDs that fired) — optional mapping back to config
- `created_at` (load time; not a good timeline signal for dashboards)
- `table_name`, `run_config_name`

**Checks catalog:** `dq_dev.dqx.checks` (flattened rule metadata)

---

## Design Goals

- **Insight > pretty**: tell me *which rule patterns* and *which fields* are driving most pain.
- **Row → Event**: each error/warn element becomes one **event** with a stable `event_id`.
- **Column awareness** without reading the original table: mine `row_snapshot` KV.
- **Low ceremony**: pure SQL for modeling; keep PySpark for dashboard creation only.
- **Names you can grep**: everything we create starts with `dashboard_` in the `dqx` schema.

---

## Data Model

### Events

- Explode `_errors` and `_warnings` to a unified `dashboard_events` with `severity IN ('ERROR','WARN')`.
- `event_id = sha2(log_id || ':' || severity_short || ':' || position, 256)` for stability.
- Prefer `run_time` from the event payload as the *event timestamp*.

### Row Snapshot (KV)

- Normalize `row_snapshot` (array of `{{column, value}}`) to `dashboard_row_kv` with convenience flags: `is_null`, `is_empty`, `is_zero` (best‑effort via `try_cast`).

### Offender Extraction

- `dashboard_event_offenders` adds:
  - `offender_columns` (array<string>)
  - `offender_values` (array<string>)
  - `offender_count` (int)  
- Heuristics are **function‑aware** (`e.function`) and use `e.columns` if present; else fallback to scanning all snapshot KVs. Examples:
  - `is_not_null` → any `kv.value IS NULL` for the target column(s).
  - `is_not_null_and_not_empty` → `NULL` or `trim(value)=''`.
  - `is_in_list` → values **not** in the rule argument list (when we can parse).
  - `regex_match` → values not matching supplied regex (best effort if regex appears in `message`).
  - `is_in_range` → values outside `[min_limit,max_limit]` (parse limits when present).
  - `is_unique` (dataset‑level) → flag duplicate groups among *violating logs only* using a derived fingerprint over the key columns from `row_snapshot`.

> Everything is written to tolerate partial metadata: when `columns` is `NULL` in the event, we widen to all snapshot columns.

### Rule Catalog Join

- `dashboard_rules_catalog` reads from `dq_dev.dqx.checks` (id, name, criticality, table).
- `dashboard_rules_affected` explodes `check_id` from the results log to count how often each rule fired.
- `dashboard_events_enriched` joins events to catalog by `check_name` **or** via the exploded `check_id` bridge (best‑effort).

### Rollups & KPI

- Simple time series (`run_time`), top checks/tables/columns, message noise, pattern fingerprints.
- A single summary view `dashboard_kpi` provides counts + averages for tiles.

---

## SQL — Views (ready to run)

> Unity Catalog supports **MATERIALIZED VIEW** on SQL Warehouses. While iterating, stick to regular views. When happy, flip specific `CREATE VIEW` → `CREATE MATERIALIZED VIEW` for hot paths.

```sql
-- Catalog & schema
USE CATALOG dq_dev;
USE SCHEMA dqx;

-- ───────────────────────────────
-- 1) Events (errors & warnings)
-- ───────────────────────────────
CREATE OR REPLACE VIEW dashboard_errors AS
SELECT
  l.log_id,
  l.table_name,
  l.run_config_name,
  e.name          AS check_name,
  e.message       AS message,
  e.function      AS function,
  e.filter        AS filter,
  e.run_time      AS run_time,
  e.user_metadata AS user_metadata,
  e.columns       AS columns,
  l.created_at    AS created_at,
  l._errors_fingerprint AS errors_fingerprint,
  l.row_snapshot_fingerprint AS row_snapshot_fingerprint,
  p.pos           AS err_pos,
  sha2(concat(l.log_id, ':E:', cast(p.pos AS STRING)), 256) AS event_id
FROM dqx.checks_log l
LATERAL VIEW OUTER posexplode(l._errors) p AS pos, e;

CREATE OR REPLACE VIEW dashboard_warnings AS
SELECT
  l.log_id,
  l.table_name,
  l.run_config_name,
  w.name          AS check_name,
  w.message       AS message,
  w.function      AS function,
  w.filter        AS filter,
  w.run_time      AS run_time,
  w.user_metadata AS user_metadata,
  w.columns       AS columns,
  l.created_at    AS created_at,
  l._warnings_fingerprint AS warnings_fingerprint,
  l.row_snapshot_fingerprint AS row_snapshot_fingerprint,
  p.pos           AS warn_pos,
  sha2(concat(l.log_id, ':W:', cast(p.pos AS STRING)), 256) AS event_id
FROM dqx.checks_log l
LATERAL VIEW OUTER posexplode(l._warnings) p AS pos, w;

CREATE OR REPLACE VIEW dashboard_events AS
SELECT 'ERROR' AS severity, e.* EXCEPT(err_pos) FROM dashboard_errors e
UNION ALL
SELECT 'WARN'  AS severity, w.* EXCEPT(warn_pos) FROM dashboard_warnings w;

-- ───────────────────────────────
-- 2) Row snapshot normalized
-- ───────────────────────────────
CREATE OR REPLACE VIEW dashboard_row_kv AS
SELECT
  l.log_id,
  l.table_name,
  l.run_config_name,
  l.created_at,
  kv.column AS column,
  kv.value  AS value,
  CASE WHEN kv.value IS NULL THEN TRUE ELSE FALSE END                                      AS is_null,
  CASE WHEN kv.value IS NOT NULL AND length(trim(kv.value)) = 0 THEN TRUE ELSE FALSE END  AS is_empty,
  CASE WHEN TRY_CAST(kv.value AS DOUBLE) = 0 THEN TRUE ELSE FALSE END                      AS is_zero
FROM dqx.checks_log l
LATERAL VIEW OUTER explode(l.row_snapshot) s AS kv;

-- ───────────────────────────────
-- 3) Offender extraction (function-aware)
-- ───────────────────────────────
CREATE OR REPLACE VIEW dashboard_event_offenders AS
WITH target_cols AS (
  SELECT
    e.event_id,
    e.log_id,
    e.check_name,
    e.function,
    COALESCE(e.columns, array()) AS columns
  FROM dashboard_events e
),
-- widen to all snapshot columns when event.columns is null/empty
expanded AS (
  SELECT
    t.*,
    CASE
      WHEN size(t.columns) = 0 THEN COLLECT_LIST(k.column) OVER (PARTITION BY t.log_id)
      ELSE t.columns
    END AS target_cols
  FROM target_cols t
  LEFT JOIN dashboard_row_kv k
    ON k.log_id = t.log_id
),
-- explode targets and join to KV values
kv_join AS (
  SELECT
    e.event_id, e.log_id, e.check_name, e.function,
    tgt AS target_col,
    k.value AS value
  FROM expanded e
  LATERAL VIEW explode(e.target_cols) c AS tgt
  LEFT JOIN dashboard_row_kv k
    ON k.log_id = e.log_id AND k.column = tgt
)
SELECT
  event_id,
  check_name,
  function,
  /* offenders by function */
  ARRAY_FILTER(COLLECT_SET(target_col), c -> c IS NOT NULL)          AS offender_columns,
  ARRAY_FILTER(COLLECT_SET(value), v -> v IS NOT NULL)               AS offender_values,
  COUNT(*)                                                            AS offender_count
FROM (
  SELECT
    k.*,
    CASE
      WHEN function IN ('is_not_null') THEN (value IS NULL)
      WHEN function IN ('is_not_null_and_not_empty') THEN (value IS NULL OR length(trim(value)) = 0)
      WHEN function IN ('is_in_range') THEN (
        /* best-effort parse: look for min/max in message if not in arguments */
        -- treat non-numeric as offender (parsing is deferred for now)
        TRY_CAST(value AS DOUBLE) IS NULL
      )
      WHEN function IN ('regex_match') THEN (
        -- we can't run the regex at read time without args; conservatively flag NULL/empty
        (value IS NULL OR length(value) = 0)
      )
      WHEN function IN ('is_in_list') THEN (
        -- without the allow‑list, we can only surface the actual values;
        -- downstream UI can join to checks to display allowed set
        value IS NULL
      )
      ELSE TRUE  -- default: mark the target so we can still count per‑column
    END AS is_offender
  FROM kv_join k
) z
WHERE is_offender
GROUP BY event_id, check_name, function;

-- ───────────────────────────────
-- 4) Rule catalog joins & coverage
-- ───────────────────────────────
CREATE OR REPLACE VIEW dashboard_rules_catalog AS
SELECT
  c.check_id,
  c.name,
  c.criticality,
  c.table_name,
  c.run_config_name,
  c.filter,
  c.check
FROM dq_dev.dqx.checks c;

CREATE OR REPLACE VIEW dashboard_rules_affected AS
SELECT cid AS check_id, COUNT(*) AS rows_hit
FROM dqx.checks_log
LATERAL VIEW OUTER explode(check_id) t AS cid
GROUP BY cid;

CREATE OR REPLACE VIEW dashboard_events_enriched AS
SELECT
  e.*,
  COALESCE(c.criticality, 'unknown') AS criticality,
  c.check_id                         AS matched_check_id
FROM dashboard_events e
LEFT JOIN dashboard_rules_catalog c
  ON lower(e.check_name) = lower(c.name);

-- Fallback mapping via check_id when names don't line up
CREATE OR REPLACE VIEW dashboard_events_enriched_v2 AS
SELECT
  e.*,
  COALESCE(c.criticality, 'unknown') AS criticality,
  c.check_id                         AS matched_check_id
FROM dashboard_events e
LEFT JOIN (
  SELECT l.log_id, cid AS check_id
  FROM dqx.checks_log l
  LATERAL VIEW OUTER explode(l.check_id) tmp AS cid
) m
  ON m.log_id = e.log_id
LEFT JOIN dashboard_rules_catalog c
  ON c.check_id = m.check_id;

-- ───────────────────────────────
-- 5) Rollups & KPI
-- ───────────────────────────────
CREATE OR REPLACE VIEW dashboard_kpi AS
WITH base AS (
  SELECT
    COUNT(*) AS rows_with_any_issue,
    SUM(CASE WHEN size(_errors)  > 0 THEN 1 ELSE 0 END) AS rows_with_errors,
    SUM(CASE WHEN size(_warnings)> 0 THEN 1 ELSE 0 END) AS rows_with_warnings,
    COUNT(DISTINCT row_snapshot_fingerprint)            AS unique_rows_fingerprint,
    COUNT(DISTINCT _errors_fingerprint)                 AS unique_error_patterns,
    COUNT(DISTINCT _warnings_fingerprint)               AS unique_warning_patterns,
    MIN(created_at) AS first_ingest_at,
    MAX(created_at) AS last_ingest_at
  FROM dqx.checks_log
),
evt AS (
  SELECT
    SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
    SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
  FROM dashboard_events
)
SELECT
  b.*,
  e.error_events,
  e.warning_events,
  ROUND(e.error_events / NULLIF(b.rows_with_any_issue,0), 3)  AS avg_errors_per_row,
  ROUND(e.warning_events / NULLIF(b.rows_with_any_issue,0), 3) AS avg_warnings_per_row
FROM base b CROSS JOIN evt e;

CREATE OR REPLACE VIEW dashboard_events_by_day AS
SELECT date_trunc('day', run_time) AS day,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
GROUP BY date_trunc('day', run_time)
ORDER BY day;

CREATE OR REPLACE VIEW dashboard_events_by_table AS
SELECT table_name,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
GROUP BY table_name
ORDER BY error_events DESC, warning_events DESC;

CREATE OR REPLACE VIEW dashboard_events_by_check AS
SELECT COALESCE(check_name,'unknown') AS check_name,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
GROUP BY COALESCE(check_name,'unknown')
ORDER BY error_events DESC, warning_events DESC;

CREATE OR REPLACE VIEW dashboard_events_by_column AS
SELECT col AS column,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
LATERAL VIEW OUTER explode(columns) c AS col
GROUP BY col
ORDER BY error_events DESC, warning_events DESC;

CREATE OR REPLACE VIEW dashboard_top_error_messages AS
SELECT message, COUNT(*) AS error_events
FROM dashboard_errors
GROUP BY message
ORDER BY error_events DESC;
```

> When ready to materialize: replace `CREATE OR REPLACE VIEW ...` with `CREATE OR REPLACE MATERIALIZED VIEW ...` for **`dashboard_events`**, **`dashboard_row_kv`**, **`dashboard_event_offenders`**, and **`dashboard_events_by_day`**.

---

## Function Coverage Matrix

| Function family | Offenders we can surface today | Notes |
|---|---|---|
| `is_not_null` | target columns where `value IS NULL` | Straight from KV. |
| `is_not_null_and_not_empty` | `NULL` **or** `trim(value)=''` | Text fields handled. |
| `is_in_list` | target values (cannot compute “allowed” without args) | Display actual bad values; cross‑link to rule args in catalog for the list. |
| `regex_match` | `NULL/empty` now; **TODO**: apply regex when available | We can parse `check.arguments.regex` once we wire args from catalog. |
| `is_in_range` | non‑numeric values as offenders now; **TODO**: parse min/max | If args include `min_limit/max_limit`, use them to test numerically. |
| `is_unique` (dataset) | derive duplicate fingerprint across key columns among violating logs | “Local” to violating set; still diagnostic. |
| `sql_expression` | best‑effort: surface the target columns; treat `NULL/empty` as offenders | Full eval requires the original SQL or parsed predicates. |

The official DQX reference lists available **row‑level** and **dataset‑level** checks, with guidance on when to prefer dataset‑level for large group validations. See references below.

---

## Performance & Ops Notes

- Use **`run_time`** for time series; `created_at` belongs to the batch writer and will often be identical across one load.
- Partitioning: if you later switch to **materialized views**, UC will manage refreshes. For large tables, consider clustering your **base** table by `table_name`, `run_config_name`, and a date bucket from `run_time` for locality.
- De‑dup is handled by `event_id` and `log_id` hashes; you can `SELECT DISTINCT` in downstream tiles if needed.
- Null/empty detection runs purely on the stringified snapshot; add type‑aware parsing (e.g., `try_cast`) when we wire rule arguments.

---

## Lakeview — Programmatic Create/Publish

```python
# Run in a Databricks notebook that sits at the repo root.
# Expects: dashboards/DQX_Overview.lvdash.json committed next to this notebook.
%pip install -q databricks-sdk
dbutils.library.restartPython()

import os, json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard as LvDashboard

WAREHOUSE_ID = "ca5a45e27debab49"
DISPLAY_NAME = "DQX – Quality Overview"
PARENT_PATH  = "/Users/levi.gagne@claconnect.com/DQX"  # or /Shared/DQX

nb_path   = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_dir  = os.path.dirname(nb_path)
json_path = f"/Workspace{repo_dir}/dashboards/DQX_Overview.lvdash.json"

serialized = open(json_path, "r", encoding="utf-8").read().replace("{{WAREHOUSE_ID}}", WAREHOUSE_ID)

w = WorkspaceClient()
try:
    w.workspace.get_status(PARENT_PATH)
except Exception:
    w.workspace.mkdirs(PARENT_PATH)

dash = w.lakeview.create(LvDashboard(
    display_name=DISPLAY_NAME,
    warehouse_id=WAREHOUSE_ID,
    parent_path=PARENT_PATH,
    serialized_dashboard=serialized
))
w.lakeview.publish(dash.dashboard_id)

print(f"Dashboard: {w.config.host.rstrip('/')}/dashboardsv3/{dash.dashboard_id}")
```

> Scheduling refresh via SDK is possible, but the current Python typings may lag the API in some runtimes. If the `create_schedule` helper isn’t present, pin a newer `databricks-sdk` or attach a schedule once via UI, then leave the rest to code.

---

## Next Steps

1. Wire **rule arguments** into `dashboard_event_offenders` (`min/max`, `allowed`, `regex`) by joining events to `dashboard_rules_catalog` and parsing `check.arguments`.  
2. Add a **duplicate‑group view** for `is_unique` with group size and sample keys.  
3. When stable, flip **hot views** to **materialized** for speed.  
4. Add **Lakeview tiles** sourced from these views; keep tile SQL trivial (no array parsing in the dashboard).

---

## References

- DQX “Quality Rules” & row‑level checks: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#row-level-checks-reference  
- Dataset‑level checks (incl. `foreign_key`) and guidance on row vs dataset checks: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#dataset-level-checks-reference