# DQX offender-extraction plan — **checks_log-first** dashboard

**Owner:** Levi Gagne  
**Scope:** Build a *useful* DQ dashboard on top of `dq_dev.dqx.checks_log` with views that turn row-level booleans into actionable counts, offenders, and drilldowns.  
**Style:** fast, correct, and usable. No BS.

---

## TL;DR

- Normalize `checks_log` into three views:
  1) `dashboard_events` — one record **per issue** (error/warn) per row, with `function`, `message`, `columns`, fingerprints, etc.  
  2) `dashboard_row_kv` — the **row snapshot** exploded into `column,value,is_null,is_empty,is_zero` for drilldowns.  
  3) `dashboard_kpi` & breakdowns — the simple rollups the dashboard needs now.

- Add **function-aware offenders** via *thin* views:
  - `dashboard_offenders_is_not_null` / `is_not_empty` / `is_not_null_and_not_empty`
  - `dashboard_offenders_is_in_list`, `regex_match`, `is_in_range`, `is_not_greater_than`, `is_not_less_than`, etc.
  - Each view emits: `event_id, table_name, run_config_name, check_name, target_column, offending_value, run_time, severity`.

- For **arguments**, prefer `checks.check.check.arguments` via `check_id` join; fall back to parsing `message` when `check_id` is missing and function is `sql_expression`.

- Start with **views** (cheap to iterate); we can flip selective ones to **materialized views** later once patterns stabilize.

---

## Contents

1. [Source & assumptions](#source--assumptions)  
2. [Ready‑to‑run SQL: base views](#ready-to-run-sql-base-views)  
3. [Function coverage matrix](#function-coverage-matrix) — definitions, args, fail semantics, and offender SQL patterns  
4. [Offender extraction views (ready to run)](#offender-extraction-views-ready-to-run)  
5. [Notes: scale & ops](#notes-scale--ops)

---

## Source & assumptions

- **Results table**: `dq_dev.dqx.checks_log` (schema you provided). It’s row‑level: one log row per *source row* that tripped ≥1 rule; `_errors`/`_warnings` are arrays of issue structs.  
- **Rules table**: `dq_dev.dqx.checks` (flattened config). We join by exploding `check_id` from results → `checks.check_id` to recover `function` and **arguments** when needed.  
- **Semantics**: DQX emits booleans; a rule “fires” when the boolean condition is **violated**. We interpret “fail = true” per row for row‑level checks. Dataset‑level checks also report per row (DQX joins results back to the input).  
- **Timestamps**: `created_at` in results is a batch write time (not event time). For trends, prefer `e.run_time` within `_errors/_warnings` if present; otherwise, fall back to `created_at`.

---

## Ready‑to‑run SQL: base views

> Run this *as-is* in a Databricks notebook (SQL or PySpark with `%sql`). Views are under `dq_dev.dqx` and prefixed with `dashboard_…` so they group in the UI.

```sql
USE CATALOG dq_dev;
USE SCHEMA dqx;

/* 1) Events: explode errors & warnings with stable event_id */
CREATE OR REPLACE VIEW dashboard_errors AS
SELECT
  'ERROR'                 AS severity,
  l.log_id,
  l.table_name,
  l.run_config_name,
  e.name                  AS check_name,
  e.message               AS message,
  e.function              AS function,
  e.filter                AS filter,
  e.run_time              AS run_time,
  e.user_metadata         AS user_metadata,
  e.columns               AS columns,
  l.created_at            AS created_at,
  l._errors_fingerprint   AS errors_fingerprint,
  l.row_snapshot_fingerprint AS row_snapshot_fingerprint,
  p.pos                   AS pos,
  sha2(concat(l.log_id, ':E:', cast(p.pos AS STRING)), 256) AS event_id
FROM dqx.checks_log l
LATERAL VIEW OUTER posexplode(_errors) p AS pos, e;

CREATE OR REPLACE VIEW dashboard_warnings AS
SELECT
  'WARN'                  AS severity,
  l.log_id,
  l.table_name,
  l.run_config_name,
  w.name                  AS check_name,
  w.message               AS message,
  w.function              AS function,
  w.filter                AS filter,
  w.run_time              AS run_time,
  w.user_metadata         AS user_metadata,
  w.columns               AS columns,
  l.created_at            AS created_at,
  l._warnings_fingerprint AS warnings_fingerprint,
  l.row_snapshot_fingerprint AS row_snapshot_fingerprint,
  p.pos                   AS pos,
  sha2(concat(l.log_id, ':W:', cast(p.pos AS STRING)), 256) AS event_id
FROM dqx.checks_log l
LATERAL VIEW OUTER posexplode(_warnings) p AS pos, w;

/* Unified events stream */
CREATE OR REPLACE VIEW dashboard_events AS
SELECT severity, log_id, table_name, run_config_name, check_name, message, function, filter, run_time,
       user_metadata, columns, created_at, row_snapshot_fingerprint, event_id
FROM dashboard_errors
UNION ALL
SELECT severity, log_id, table_name, run_config_name, check_name, message, function, filter, run_time,
       user_metadata, columns, created_at, row_snapshot_fingerprint, event_id
FROM dashboard_warnings;

/* 2) Row snapshot KV for offenders */
CREATE OR REPLACE VIEW dashboard_row_kv AS
SELECT
  l.log_id,
  l.table_name,
  l.run_config_name,
  l.created_at,
  kv.column                         AS column,
  kv.value                          AS value,
  kv.value IS NULL                  AS is_null,
  (kv.value IS NOT NULL AND length(trim(kv.value)) = 0) AS is_empty,
  (safe_cast(kv.value AS DOUBLE) = 0) AS is_zero
FROM dqx.checks_log l
LATERAL VIEW OUTER explode(row_snapshot) s AS kv;

/* 3) Lightweight KPIs */
CREATE OR REPLACE VIEW dashboard_kpi AS
WITH base AS (
  SELECT
    COUNT(*) AS rows_with_any_issue,
    SUM(CASE WHEN size(_errors)  > 0 THEN 1 ELSE 0 END) AS rows_with_errors,
    SUM(CASE WHEN size(_warnings)> 0 THEN 1 ELSE 0 END) AS rows_with_warnings,
    COUNT(DISTINCT row_snapshot_fingerprint)            AS unique_rows_fingerprint,
    COUNT(DISTINCT _errors_fingerprint)                 AS unique_error_patterns,
    COUNT(DISTINCT _warnings_fingerprint)               AS unique_warning_patterns
  FROM dqx.checks_log
),
evt AS (
  SELECT
    SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
    SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
  FROM dashboard_events
)
SELECT b.*, e.*,
       ROUND(e.error_events   / NULLIF(b.rows_with_any_issue,0), 3) AS avg_errors_per_row,
       ROUND(e.warning_events / NULLIF(b.rows_with_any_issue,0), 3) AS avg_warnings_per_row
FROM base b CROSS JOIN evt e;

/* Breakdown helpers */
CREATE OR REPLACE VIEW dashboard_events_by_day AS
SELECT coalesce(date_trunc('day', run_time), date_trunc('day', created_at)) AS day,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
GROUP BY coalesce(date_trunc('day', run_time), date_trunc('day', created_at))
ORDER BY day;

CREATE OR REPLACE VIEW dashboard_events_by_table AS
SELECT table_name,
       SUM(CASE WHEN severity='ERROR' THEN 1 ELSE 0 END) AS error_events,
       SUM(CASE WHEN severity='WARN'  THEN 1 ELSE 0 END) AS warning_events
FROM dashboard_events
GROUP BY table_name
ORDER BY error_events DESC, warning_events DESC;
```

---

## Function coverage matrix

> Based on **Databricks Labs DQX** docs. I’m paraphrasing the official descriptions and listing the key arguments you’ll see in `checks.check.check.arguments` from the config table (when `check_id` links), plus how I interpret **fail** semantics and how to extract **offending values** from `row_snapshot`.\
> Source: DQX *Quality Rules* — Row‑level & Dataset‑level checks.

### Row‑level checks

| Function | Official summary | Key args | “Fail” means | Offender SQL pattern |
|---|---|---|---|---|
| `is_not_null` | Values in `column` are **not null**. | `column` | Value **IS NULL**. | Join `dashboard_events` by `function='is_not_null'` → restrict `dashboard_row_kv` to `column` (from args if available, else any) and `is_null=true` → emit `(event_id, column, value)`. |
| `is_not_empty` | Values in `column` are **not empty** (after trim). | `column` | Value is **empty string**. | Same join; filter `is_empty=true`. |
| `is_not_null_and_not_empty` | Value in `column` is neither `NULL` nor **empty**. | `column` | `is_null OR is_empty`. | Filter `(is_null OR is_empty)`. |
| `is_in_list` | Values in `column` exist in **allowed** list (array or table). | `column`, `allowed` (list) | `value NOT IN allowed` (ignoring null/empty per policy). | Parse `allowed` from config args; then `value NOT IN allowed AND NOT is_null AND NOT is_empty`. |
| `regex_match` | Value in `column` **matches regex**. | `column`, `regex` | `NOT regexp_like(value, regex)`. | Use `NOT regexp_like(value, <regex>)` with null/empty guards. |
| `is_in_range` | Value in `column` within [`min_limit`,`max_limit`] (numeric/date). | `column`, `min_limit`, `max_limit` | `value < min OR value > max` (respect inclusive flags if present). | Cast to DECIMAL/DATE per arg types, compare vs limits. |
| `is_not_greater_than` | Value in `column` is **≤ limit**. | `column`, `limit` | `value > limit`. | Cast numeric/date; filter `value > limit`. |
| `is_not_less_than` | Value in `column` is **≥ limit**. | `column`, `limit` | `value < limit`. | Cast numeric/date; filter `value < limit`. |
| `is_equal_to` | Value in `column` equals `ref_value`/`ref_column`. | `column`, `ref_value` or `ref_column` | `value != ref`. | Compare against literal or second column pulled from KV. |
| `is_not_equal_to` | Value in `column` **!= ref**. | `column`, `ref_value` or `ref_column` | `value == ref`. | Same as above, negated. |
| `is_greater_than` | Value in `column` **> limit/ref**. | `column`, `limit` or `ref_column` | `value <= ref`. | Numeric/date compare. |
| `is_less_than` | Value in `column` **< limit/ref**. | `column`, `limit` or `ref_column` | `value >= ref`. | Numeric/date compare. |
| `is_older_than_n_days` | Value in time `column1` is at least **n days older than** `column2`. | `column1`, `column2`, `days` | `DATEDIFF(column2, column1) < days`. | Cast both as date/timestamp, compute `datediff`. |
| `sql_expression` | Free‑form boolean **SQL expression**; fail when expression evaluates **False** (or `negate` flips). | `expression` (string), optional `negate`, `name`, `msg` | Depends on expr. | When `check_id` missing, we can only surface `message` and not auto‑derive offenders; otherwise parse the expression or join back to args. |

### Dataset‑level checks

| Function | Official summary | Key args | “Fail” means | Offender SQL pattern |
|---|---|---|---|---|
| `is_unique` | Values in `columns` are **unique** (composite key supported; nulls distinct by default). | `columns`, `nulls_distinct` | A key repeats. | Compute `COUNT(*) OVER (PARTITION BY key)`; offenders have count > 1. |
| `is_aggr_not_greater_than` | Aggregated values **≤ limit** (count/sum/avg/min/max). | `column` (optional for `count`), `limit`, `aggr_type`, `group_by` | Aggregate > limit. | Use aggregation over `group_by` slice, flag groups violating limit. |
| `is_aggr_not_less_than` | Aggregated values **≥ limit**. | same as above | Aggregate < limit. | Mirror of above. |
| `is_aggr_equal` | Aggregate equals limit. | same | Aggregate != limit. | Equality compare. |
| `is_aggr_not_equal` | Aggregate != limit. | same | Aggregate == limit. | Negated equality. |
| `foreign_key` | Input `columns` exist in **reference** (DF or table). | `columns`, `ref_columns`, `ref_df_name`/`ref_table`, `negate` | Value missing (or present when `negate=true`). | Anti‑join against reference. |
| `sql_query` | Custom SQL that returns a **condition column** (True = fail). | `query`, `input_placeholder`, `merge_columns`, `condition_column`, … | Rows where condition is True. | Use the provided query and merge cols. |
| `compare_datasets` | Compare two DFs by `columns` and cell diffs; attaches a JSON diff log into `message`. | `columns`, `ref_columns`, `exclude_columns`, `ref_df_name`/`ref_table`, … | Any missing/extra/mismatched. | Parse the structured JSON in `message`→`dataset_diffs.changed` to render specifics. |
| `is_data_fresh_per_time_window` | At least *X* records per *Y*-minute window. | `column`, `window_minutes`, `min_records_per_window`, … | Any window with fewer than min records. | Group by tumbling windows and count. |

> I’ll wire offenders for these on demand; most dataset checks don’t use `row_snapshot` by design.

---

## Offender extraction views (ready to run)

> These assume you’ll have rule arguments in `dq_dev.dqx.checks` and that **most** row‑level checks target a *single* column (common case). When `check_id` is missing, we fall back to heuristics (regex/message) only for `sql_expression`.

```sql
USE CATALOG dq_dev;
USE SCHEMA dqx;

/* Helper: explode check_id to join rules */
CREATE OR REPLACE VIEW dashboard_event_rules AS
SELECT e.*, cid AS check_id
FROM dashboard_events e
LATERAL VIEW OUTER explode(
  coalesce((SELECT check_id FROM dqx.checks_log l WHERE l.log_id = e.log_id), array())
) t AS cid;

CREATE OR REPLACE VIEW dashboard_rules_catalog AS
SELECT check_id, name, criticality, table_name, run_config_name,
       check.function AS rule_function,
       check.for_each_column AS rule_for_each_column,
       check.arguments AS rule_arguments
FROM dq_dev.dqx.checks;

/* === 1) is_not_null === */
CREATE OR REPLACE VIEW dashboard_offenders_is_not_null AS
WITH targets AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         /* prefer explicit target(s) from config, else try errors.columns, else any column */
         coalesce(rc.rule_for_each_column, er.columns) AS target_cols
  FROM dashboard_event_rules er
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_not_null'
)
SELECT
  t.event_id, t.table_name, t.run_config_name, t.check_name, t.run_time, t.severity,
  kv.column       AS target_column,
  kv.value        AS offending_value
FROM targets t
JOIN dashboard_row_kv kv ON kv.log_id = t.log_id
WHERE (target_cols IS NULL OR array_contains(target_cols, kv.column))
  AND kv.is_null = TRUE;

/* === 2) is_not_empty === */
CREATE OR REPLACE VIEW dashboard_offenders_is_not_empty AS
WITH targets AS (
  SELECT er.*, coalesce(rc.rule_for_each_column, er.columns) AS target_cols
  FROM dashboard_event_rules er
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_not_empty'
)
SELECT t.event_id, t.table_name, t.run_config_name, t.check_name, t.run_time, t.severity,
       kv.column AS target_column, kv.value AS offending_value
FROM targets t
JOIN dashboard_row_kv kv ON kv.log_id = t.log_id
WHERE (target_cols IS NULL OR array_contains(target_cols, kv.column))
  AND kv.is_empty = TRUE;

/* === 3) is_not_null_and_not_empty === */
CREATE OR REPLACE VIEW dashboard_offenders_is_not_null_and_not_empty AS
WITH targets AS (
  SELECT er.*, coalesce(rc.rule_for_each_column, er.columns) AS target_cols
  FROM dashboard_event_rules er
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_not_null_and_not_empty'
)
SELECT t.event_id, t.table_name, t.run_config_name, t.check_name, t.run_time, t.severity,
       kv.column AS target_column, kv.value AS offending_value
FROM targets t
JOIN dashboard_row_kv kv ON kv.log_id = t.log_id
WHERE (target_cols IS NULL OR array_contains(target_cols, kv.column))
  AND (kv.is_null OR kv.is_empty);

/* === 4) is_in_list === */
CREATE OR REPLACE VIEW dashboard_offenders_is_in_list AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         kv.column, kv.value,
         try_from_json(rc.rule_arguments['allowed']) AS allowed_json /* string->array */
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_in_list'
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column AS target_column, value AS offending_value
FROM args
WHERE value IS NOT NULL AND length(trim(value))>0
  AND (allowed_json IS NULL OR NOT array_contains(allowed_json, value));

/* === 5) regex_match === */
CREATE OR REPLACE VIEW dashboard_offenders_regex_match AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         kv.column, kv.value,
         rc.rule_arguments['regex'] AS regex
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'regex_match'
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column AS target_column, value AS offending_value
FROM args
WHERE value IS NOT NULL AND length(trim(value))>0
  AND (regex IS NULL OR NOT regexp_like(value, regex));

/* === 6) is_in_range === */
CREATE OR REPLACE VIEW dashboard_offenders_is_in_range AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         kv.column, kv.value,
         safe_cast(rc.rule_arguments['min_limit'] AS DOUBLE) AS min_limit,
         safe_cast(rc.rule_arguments['max_limit'] AS DOUBLE) AS max_limit
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_in_range'
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column AS target_column, value AS offending_value
FROM args
WHERE value IS NOT NULL AND try_cast(value AS DOUBLE) IS NOT NULL
  AND (
    (min_limit IS NOT NULL AND try_cast(value AS DOUBLE) <  min_limit) OR
    (max_limit IS NOT NULL AND try_cast(value AS DOUBLE) >  max_limit)
  );

/* === 7) is_not_greater_than === */
CREATE OR REPLACE VIEW dashboard_offenders_is_not_greater_than AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         kv.column, kv.value,
         safe_cast(rc.rule_arguments['limit'] AS DOUBLE) AS limit
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_not_greater_than'
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column AS target_column, value AS offending_value
FROM args
WHERE value IS NOT NULL AND try_cast(value AS DOUBLE) IS NOT NULL
  AND (limit IS NULL OR try_cast(value AS DOUBLE) > limit);

/* === 8) is_not_less_than === */
CREATE OR REPLACE VIEW dashboard_offenders_is_not_less_than AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         kv.column, kv.value,
         safe_cast(rc.rule_arguments['limit'] AS DOUBLE) AS limit
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_not_less_than'
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column AS target_column, value AS offending_value
FROM args
WHERE value IS NOT NULL AND try_cast(value AS DOUBLE) IS NOT NULL
  AND (limit IS NULL OR try_cast(value AS DOUBLE) < limit);

/* === 9) is_equal_to / is_not_equal_to / is_greater_than / is_less_than ===
     If rule_arguments contains a literal (e.g., 'ref_value'), we compare to it.
     If rule_arguments carries 'ref_column', we lookup that column in row_snapshot. */
CREATE OR REPLACE VIEW dashboard_offenders_value_compare AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity, er.function,
         kv.column AS target_column, kv.value AS left_value,
         rc.rule_arguments['ref_value'] AS ref_value_str,
         rc.rule_arguments['ref_column'] AS ref_column_name
  FROM dashboard_event_rules er
  JOIN dashboard_row_kv kv ON kv.log_id = er.log_id
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function IN ('is_equal_to','is_not_equal_to','is_greater_than','is_less_than')
),
ref AS (
  SELECT a.*, rv.value AS ref_column_value
  FROM args a
  LEFT JOIN dashboard_row_kv rv
    ON rv.log_id = a.log_id AND rv.column = a.ref_column_name
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity, function,
       target_column,
       left_value AS offending_value
FROM ref
WHERE
  CASE function
    WHEN 'is_equal_to'       THEN coalesce(left_value, '') <> coalesce(coalesce(ref_value_str, ref_column_value), '')
    WHEN 'is_not_equal_to'   THEN coalesce(left_value, '')  = coalesce(coalesce(ref_value_str, ref_column_value), '')
    WHEN 'is_greater_than'   THEN try_cast(left_value AS DOUBLE) <= try_cast(coalesce(ref_value_str, ref_column_value) AS DOUBLE)
    WHEN 'is_less_than'      THEN try_cast(left_value AS DOUBLE) >= try_cast(coalesce(ref_value_str, ref_column_value) AS DOUBLE)
  END;

/* === 10) is_older_than_n_days === */
CREATE OR REPLACE VIEW dashboard_offenders_is_older_than_n_days AS
WITH args AS (
  SELECT er.event_id, er.log_id, er.table_name, er.run_config_name, er.check_name, er.run_time, er.severity,
         rc.rule_arguments['column1'] AS column1,
         rc.rule_arguments['column2'] AS column2,
         safe_cast(rc.rule_arguments['days'] AS INT) AS days
  FROM dashboard_event_rules er
  LEFT JOIN dashboard_rules_catalog rc USING (check_id)
  WHERE er.function = 'is_older_than_n_days'
),
kv AS (
  SELECT a.*, v1.value AS v1, v2.value AS v2
  FROM args a
  LEFT JOIN dashboard_row_kv v1 ON v1.log_id=a.log_id AND v1.column=a.column1
  LEFT JOIN dashboard_row_kv v2 ON v2.log_id=a.log_id AND v2.column=a.column2
)
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       column1 AS target_column, v1 AS offending_value
FROM kv
WHERE datediff(to_date(v2), to_date(v1)) < coalesce(days, 0);

/* === 11) sql_expression — surface only (opaque) === */
CREATE OR REPLACE VIEW dashboard_offenders_sql_expression AS
SELECT event_id, table_name, run_config_name, check_name, run_time, severity,
       /* No safe generic extraction: surface message and let drilldowns use row_kv */
       message AS expression_or_message
FROM dashboard_events
WHERE function = 'sql_expression';
```

---

## Notes: scale & ops

- **Events fan‑out**: you’ll cap events by rules; that’s fine. If a rule targets N columns via `for_each_column`, `dashboard_event_rules` keeps those mappings.
- **Arguments shape**: in config they’re `map<string,string>`. I coerce numerics with `safe_cast` and parse arrays with `try_from_json` where needed.
- **When `check_id` is empty**: offenders views still work for *null/empty* families using heuristics on `row_snapshot`. For *value‑based* functions (`is_in_list`, ranges), argument‑driven views require the join to populate.
- **Materialize later**: once stable, flip the heavy ones to `CREATE MATERIALIZED VIEW …` and point Lakeview tiles to those for speed.

---

## Appendix — doc pointers I followed

- Row‑level checks list, args and examples (incl. complex types and `sql_expression`).  
- Dataset‑level checks: `is_unique`, aggregation guards, `foreign_key`, `sql_query`, `compare_datasets`, `is_data_fresh_per_time_window`.

```text
DQX docs — Quality Rules
Row-level: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#row-level-checks-reference
Dataset-level: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#dataset-level-checks-reference
```