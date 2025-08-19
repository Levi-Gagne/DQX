# DQX Quality Analytics — Base Model & Query Pack (v0.8.x)

This starter pack documents how to go from the raw DQX **text log** (`dq_dev.dqx.checks_log`) and **rules catalog** (`dq_dev.dqx.checks`) to analytics‑ready **materialized views** and **function‑specific query recipes** for both **row‑level** and **dataset‑level** checks.

> **Scope:** Designed for our current layout where `checks_log` stores arrays of issues (`_errors`, `_warnings`) per offending row and **each issue element already contains** a `check_id` pointing back to the canonical rule in `checks`. We keep our existing table as‑is and add derived MVs for fast slicing and drill‑downs.

---

## Table of Contents

- [1. ER‑Style Mapping](#1-er-style-mapping)
- [2. Naming & Locations](#2-naming--locations)
- [3. Base Materialized Views (idempotent)](#3-base-materialized-views-idempotent)
  - [3.1 `dashboard_event_base_mv`](#31-dashboard_event_base_mv)
  - [3.2 `dashboard_row_kv_mv`](#32-dashboard_row_kv_mv)
  - [3.3 `dashboard_row_value_stats_mv`](#33-dashboard_row_value_stats_mv)
  - [3.4 `dashboard_rule_map_mv`](#34-dashboard_rule_map_mv)
  - [3.5 `dashboard_events_mv`](#35-dashboard_events_mv)
- [4. Analytics Patterns — Row Level](#4-analytics-patterns--row-level)
- [5. Analytics Patterns — Dataset Level](#5-analytics-patterns--dataset-level)
- [6. Function Coverage Matrix](#6-function-coverage-matrix)
  - [6.1 Row‑Level Functions](#61-rowlevel-functions)
  - [6.2 Dataset‑Level Functions](#62-datasetlevel-functions)
- [7. Query Library (Snippets per Function)](#7-query-library-snippets-per-function)
  - [7.1 Row Level](#71-row-level)
  - [7.2 Dataset Level](#72-dataset-level)
- [8. Dashboard Wiring Tips](#8-dashboard-wiring-tips)
- [9. Performance & Partitioning Notes](#9-performance--partitioning-notes)
- [10. Change Log](#10-change-log)

---

## 1. ER‑Style Mapping

```
+---------------------------+          +----------------------+
| dq_dev.dqx.checks         |          | dq_dev.dqx.checks_log|
|---------------------------|          |----------------------|
| check_id (PK)             |  <----+  | log_id (PK)          |
| table_name                |       |  | run_config_name      |
| run_config_name           |       |  | table_name           |
| name (rule name)          |       |  | _errors  (ARRAY<issue>) 
| criticality               |       |  | _warnings(ARRAY<issue>) 
| filter                    |       |  | row_snapshot(ARRAY<KV>) 
| check STRUCT              |       |  | created_at           |
|   function                |       |  | ...                  |
|   for_each_column[]       |       |  +----------------------+
|   arguments MAP           |       |
+---------------------------+       |
                                    |
        (lookup by check_id)        |
              +---------------------+
              |
              v
+--------------------------------------------+
| dashboard_event_base_mv                     |
|---------------------------------------------|
| event_id (PK)                               |
| log_id, severity ('ERROR'|'WARN')           |
| table_name, run_config_name, created_at     |
| check_id (from each issue)                  |
| check_name, message, function, filter       |
| columns ARRAY<STRING>, run_time, user_metadata MAP |
+--------------------------------------------+
       |
       | explode(row_snapshot)                         +-------------------------+
       |                                               | dashboard_rule_map_mv   |
       v                                               |-------------------------|
+--------------------------------------------+        | check_id, table_name,   |
| dashboard_row_kv_mv                        |        | run_config_name, name,  |
|--------------------------------------------|        | criticality, function,  |
| log_id, table_name, run_config_name        |        | filter, arguments JSON  |
| column, value (STRING),                    |        +-------------------------+
| is_null, is_empty, is_zero                 |
+--------------------------------------------+
       |
       v
+--------------------------------------------+
| dashboard_row_value_stats_mv               |
|--------------------------------------------|
| log_id, null_count, empty_count, zero_count|
+--------------------------------------------+

(Convenience)
+--------------------------------------------+
| dashboard_events_mv                         |
|---------------------------------------------|
| UNION of ERROR/WARN with consistent schema  |
+--------------------------------------------+
```

**Why this shape?**  
- `checks_log` stays compact and faithful to the engine output.  
- `dashboard_*_mv` provides exploded, columnar access for fast BI queries.  
- `check_id` acts as a **foreign key** from any event to its canonical rule in `checks`.

---

## 2. Naming & Locations

- **Catalog/Schema:** `dq_dev.dqx` (adjust if needed).
- **Source tables**
  - `dq_dev.dqx.checks` — rules/config table (unique `check_id`).
  - `dq_dev.dqx.checks_log` — text log with `_errors`/`_warnings` arrays (each issue includes `check_id`).

- **Derived materialized views** (created below)
  - `dashboard_event_base_mv`
  - `dashboard_row_kv_mv`
  - `dashboard_row_value_stats_mv`
  - `dashboard_rule_map_mv`
  - `dashboard_events_mv`

---

## 3. Base Materialized Views (idempotent)

> Run once; safe to re‑run. If we prefer ordinary views, replace `MATERIALIZED VIEW` with `VIEW`.

> **Assumptions:** `_errors[].check_id` and `_warnings[].check_id` exist in `checks_log` (per the updated writer we implemented). If some historical rows lack it, the queries still work; those events will have `check_id = NULL` until backfilled.

### 3.1 `dashboard_event_base_mv`

Explodes `_errors` and `_warnings` into a single **event** stream, preserving `check_id` and all issue fields.

```sql
USE CATALOG dq_dev;
USE SCHEMA dqx;

-- Drop-and-create for idempotence
DROP MATERIALIZED VIEW IF EXISTS dashboard_event_base_mv;

CREATE MATERIALIZED VIEW dashboard_event_base_mv AS
WITH E AS (
  SELECT
    l.log_id,
    'ERROR'                         AS severity,
    l.table_name,
    l.run_config_name,
    l.created_at,
    e.check_id                      AS check_id,
    e.name                          AS check_name,
    e.message,
    e.columns,
    e.filter,
    e.function,
    e.run_time,
    e.user_metadata
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
),
W AS (
  SELECT
    l.log_id,
    'WARN'                          AS severity,
    l.table_name,
    l.run_config_name,
    l.created_at,
    w.check_id                      AS check_id,
    w.name                          AS check_name,
    w.message,
    w.columns,
    w.filter,
    w.function,
    w.run_time,
    w.user_metadata
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_warnings) t AS w
)
SELECT
  -- deterministic per (log_id, pos) wasn't captured here; compute a stable surrogate:
  sha2(concat_ws('||', log_id, severity, coalesce(check_id,''), coalesce(check_name,''), coalesce(function,''), coalesce(filter,''), coalesce(message,''))), 256) AS event_id,
  *
FROM (SELECT * FROM E UNION ALL SELECT * FROM W);
```

### 3.2 `dashboard_row_kv_mv`

Explodes the row snapshot into key/value for column‑level slicing.

```sql
DROP MATERIALIZED VIEW IF EXISTS dashboard_row_kv_mv;

CREATE MATERIALIZED VIEW dashboard_row_kv_mv AS
SELECT
  l.log_id,
  l.table_name,
  l.run_config_name,
  l.created_at,
  kv.column AS column,
  kv.value  AS value,
  CASE WHEN kv.value IS NULL THEN TRUE ELSE FALSE END                                    AS is_null,
  CASE WHEN kv.value IS NOT NULL AND length(trim(kv.value)) = 0 THEN TRUE ELSE FALSE END AS is_empty,
  CASE WHEN TRY_CAST(kv.value AS DOUBLE) = 0 THEN TRUE ELSE FALSE END                    AS is_zero
FROM dq_dev.dqx.checks_log l
LATERAL VIEW OUTER explode(row_snapshot) s AS kv;
```

### 3.3 `dashboard_row_value_stats_mv`

Per offending row, a count of basic value states — useful for “how many nulls per row” style KPIs.

```sql
DROP MATERIALIZED VIEW IF EXISTS dashboard_row_value_stats_mv;

CREATE MATERIALIZED VIEW dashboard_row_value_stats_mv AS
SELECT
  log_id,
  SUM(CASE WHEN is_null  THEN 1 ELSE 0 END) AS null_count,
  SUM(CASE WHEN is_empty THEN 1 ELSE 0 END) AS empty_count,
  SUM(CASE WHEN is_zero  THEN 1 ELSE 0 END) AS zero_count
FROM dashboard_row_kv_mv
GROUP BY log_id;
```

### 3.4 `dashboard_rule_map_mv`

Thin index of the rules table we join to by `check_id` for parameters/metadata.

```sql
DROP MATERIALIZED VIEW IF EXISTS dashboard_rule_map_mv;

CREATE MATERIALIZED VIEW dashboard_rule_map_mv AS
SELECT
  check_id,
  lower(table_name)     AS table_name_norm,
  table_name,
  run_config_name,
  name                  AS rule_name,
  criticality,
  check.function        AS function,
  filter,
  -- arguments come from our config as MAP<STRING,STRING> (or STRUCT).
  -- Keep as JSON for flexible parsing by function-specific SQL:
  to_json(check.arguments) AS arguments_json
FROM dq_dev.dqx.checks
WHERE active = TRUE;
```

### 3.5 `dashboard_events_mv`

A convenience union for fast trend tiles.

```sql
DROP MATERIALIZED VIEW IF EXISTS dashboard_events_mv;

CREATE MATERIALIZED VIEW dashboard_events_mv AS
SELECT * FROM dashboard_event_base_mv;
```

---

## 4. Analytics Patterns — Row Level

- **Count offenders by column**: join `dashboard_event_base_mv` to `dashboard_row_kv_mv` and intersect with `columns` when the rule targets specific columns; otherwise treat all columns as candidates or scope via the rules table.
- **Per‑row null density**: join `dashboard_row_value_stats_mv` for KPIs like “rows with ≥ N nulls”.
- **Parameterized rules**: join `dashboard_rule_map_mv` by `check_id` to parse `arguments_json` (e.g., `min_limit`, `max_limit`, `allowed`).

---

## 5. Analytics Patterns — Dataset Level

- Dataset checks like `is_unique`, `sql_query`, `compare_datasets` usually return offenders without a single target column (or with a **set** of columns).  
- Use event rows (`dashboard_event_base_mv`) grouped by `check_id` + `table_name` to find affected rows; recompute exact deltas via a **replay query** (templated SQL) if we need ground‑truth lists, but the log‑based counts are typically sufficient for dashboards.

---

## 6. Function Coverage Matrix

Below is the reference of all **built‑in** checks in DQX 0.8.x with short guidance. For full extraction recipes see [7. Query Library](#7-query-library-snippets-per-function).

### 6.1 Row‑Level Functions

| Function | Offender Semantics (informal) | Needs from `arguments_json` |
|---|---|---|
| `is_not_null` | `value IS NULL` | `column` |
| `is_not_empty` | `value is empty string` | `column` |
| `is_not_null_and_not_empty` | `value IS NULL OR empty` | `column`, `trim_strings?` |
| `is_in_list` | `value NOT IN allowed` (nulls OK) | `column`, `allowed[]` |
| `is_not_null_and_is_in_list` | `value IS NULL OR NOT IN allowed` | `column`, `allowed[]` |
| `is_not_null_and_not_empty_array` | `array IS NULL OR size=0` | `column` |
| `is_in_range` | `value < min OR value > max` | `column`, `min_limit`, `max_limit`, `inclusive_*?` |
| `is_not_in_range` | `value BETWEEN min AND max` | same as above |
| `is_not_less_than` | `value < limit` | `column`, `limit` |
| `is_not_greater_than` | `value > limit` | `column`, `limit` |
| `is_valid_date` | `to_date(value, fmt) IS NULL` | `column`, `date_format?` |
| `is_valid_timestamp` | `to_timestamp(value, fmt) IS NULL` | `column`, `timestamp_format?` |
| `is_not_in_future` | `value > now + offset` | `column`, `offset`, `curr_timestamp?` |
| `is_not_in_near_future` | `now < value < now+offset` | `column`, `offset`, `curr_timestamp?` |
| `is_older_than_n_days` | `datediff(curr_date, value) < days` | `column`, `days`, `curr_date?`, `negate?` |
| `is_older_than_col2_for_n_days` | `datediff(col2, col1) < days` | `column1`, `column2`, `days`, `negate?` |
| `regex_match` | `NOT regexp_like(value, regex)` (unless `negate`) | `column`, `regex`, `negate?` |
| `is_valid_ipv4_address` | invalid IPv4 literal | `column` |
| `is_ipv4_address_in_cidr` | value not in CIDR | `column`, `cidr_block` |
| `sql_expression` | expression evaluates to TRUE (fail) | `expression`, `negate?`, `columns?` |
| `is_data_fresh` | `base_ts - value > max_age_minutes` | `column`, `max_age_minutes`, `base_timestamp?` |

### 6.2 Dataset‑Level Functions

| Function | Offender Semantics | Needs from `arguments_json` |
|---|---|---|
| `is_unique` | duplicate key(s) present | `columns[]`, `nulls_distinct?` |
| `is_aggr_not_greater_than` | aggr > limit | `aggr_type`, `limit`, `column?`, `group_by?` |
| `is_aggr_not_less_than` | aggr < limit | same |
| `is_aggr_equal` | aggr != limit | same |
| `is_aggr_not_equal` | aggr = limit | same |
| `foreign_key` (aka `is_in_list` dataset) | ref lookup misses | `columns[]`, `ref_*` |
| `sql_query` | condition_column TRUE | `query`, `merge_columns[]`, `condition_column`, `input_placeholder?`, `negate?` |
| `compare_datasets` | row/column diffs found | `columns[]`, `ref_columns[]`, `exclude_columns?`, `check_missing_records?` |
| `is_data_fresh_per_time_window` | window count < minimum | `column`, `window_minutes`, `min_records_per_window`, `lookback_windows?` |

---

## 7. Query Library (Snippets per Function)

> All snippets assume:
> - Base event stream: `dashboard_event_base_mv` aliased as `ev`.
> - Row KV: `dashboard_row_kv_mv` aliased as `kv`.
> - Rules index: `dashboard_rule_map_mv` aliased as `rm`.
> - For functions needing parameters, we parse `rm.arguments_json` via `from_json` using an ad‑hoc schema.

### 7.1 Row Level

#### `is_not_null` — offenders by column

```sql
WITH arg AS (
  SELECT check_id,
         from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function = 'is_not_null'
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function = 'is_not_null'
)
SELECT table_name, check_name, column, COUNT(*) AS null_rows
FROM e
JOIN arg ON arg.check_id = e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND value IS NULL
GROUP BY table_name, check_name, column
ORDER BY null_rows DESC;
```

#### `is_not_empty`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='is_not_empty'
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function = 'is_not_empty'
)
SELECT table_name, check_name, column, COUNT(*) AS empty_rows
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND value IS NOT NULL AND length(trim(value)) = 0
GROUP BY table_name, check_name, column
ORDER BY empty_rows DESC;
```

#### `is_not_null_and_not_empty`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='is_not_null_and_not_empty'
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function = 'is_not_null_and_not_empty'
)
SELECT table_name, check_name, column,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_rows,
       SUM(CASE WHEN value IS NOT NULL AND length(trim(value)) = 0 THEN 1 ELSE 0 END) AS empty_rows
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
GROUP BY table_name, check_name, column
ORDER BY null_rows DESC, empty_rows DESC;
```

#### `is_in_list`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='is_in_list'
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function = 'is_in_list'
)
SELECT table_name, check_name, column, COUNT(*) AS offenders
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND value IS NOT NULL
  AND NOT array_contains(from_json(args['allowed'], 'ARRAY<STRING>'), value)
GROUP BY table_name, check_name, column
ORDER BY offenders DESC;
```

#### `is_not_null_and_is_in_list`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='is_not_null_and_is_in_list'
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function = 'is_not_null_and_is_in_list'
)
SELECT table_name, check_name, column, COUNT(*) AS offenders
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND (value IS NULL OR NOT array_contains(from_json(args['allowed'], 'ARRAY<STRING>'), value))
GROUP BY table_name, check_name, column
ORDER BY offenders DESC;
```

#### `is_in_range` / `is_not_in_range` / `is_not_less_than` / `is_not_greater_than`

```sql
WITH arg AS (
  SELECT check_id, function,
         from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function IN ('is_in_range','is_not_in_range','is_not_less_than','is_not_greater_than')
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, ev.function, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function IN ('is_in_range','is_not_in_range','is_not_less_than','is_not_greater_than')
)
SELECT table_name, check_name, function, column, COUNT(*) AS offenders
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND (
    (function='is_in_range'      AND (CAST(value AS DOUBLE) < CAST(args['min_limit'] AS DOUBLE)
                                   OR CAST(value AS DOUBLE) > CAST(args['max_limit'] AS DOUBLE)))
 OR (function='is_not_in_range'  AND (CAST(value AS DOUBLE) BETWEEN CAST(args['min_limit'] AS DOUBLE) AND CAST(args['max_limit'] AS DOUBLE)))
 OR (function='is_not_less_than' AND (CAST(value AS DOUBLE) < CAST(args['limit'] AS DOUBLE)))
 OR (function='is_not_greater_than' AND (CAST(value AS DOUBLE) > CAST(args['limit'] AS DOUBLE)))
  )
GROUP BY table_name, check_name, function, column
ORDER BY offenders DESC;
```

#### `is_valid_date` / `is_valid_timestamp`

```sql
WITH arg AS (
  SELECT check_id, function, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function IN ('is_valid_date','is_valid_timestamp')
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, ev.function, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function IN ('is_valid_date','is_valid_timestamp')
)
SELECT table_name, check_name, function, column, COUNT(*) AS offenders
FROM e JOIN arg ON arg.check_id=e.check_id
WHERE (args['column'] IS NULL OR column = args['column'])
  AND (
    (function='is_valid_date'      AND to_date(value, coalesce(args['date_format'],'yyyy-MM-dd')) IS NULL)
 OR (function='is_valid_timestamp' AND to_timestamp(value, coalesce(args['timestamp_format'],'yyyy-MM-dd HH:mm:ss')) IS NULL)
  )
GROUP BY table_name, check_name, function, column
ORDER BY offenders DESC;
```

#### Time‑windowed: `is_not_in_future`, `is_not_in_near_future`, `is_data_fresh`, `is_older_than_n_days`, `is_older_than_col2_for_n_days`

```sql
WITH arg AS (
  SELECT check_id, function, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function IN ('is_not_in_future','is_not_in_near_future','is_data_fresh','is_older_than_n_days','is_older_than_col2_for_n_days')
),
kv_pivot AS (
  -- pivot row_snapshot as needed (only for the dual-column case)
  SELECT log_id,
         MAX(CASE WHEN column = args_map['column1'] THEN value END) AS col1_val,
         MAX(CASE WHEN column = args_map['column2'] THEN value END) AS col2_val
  FROM (
    SELECT kv.*, a.check_id, from_json(a.arguments_json,'MAP<STRING,STRING>') AS args_map
    FROM dashboard_row_kv_mv kv
    JOIN dashboard_rule_map_mv a ON kv.table_name = a.table_name AND a.function='is_older_than_col2_for_n_days'
  )
  GROUP BY log_id
),
e AS (
  SELECT ev.log_id, ev.table_name, ev.check_id, ev.check_name, ev.function, kv.column, kv.value
  FROM dashboard_event_base_mv ev
  JOIN dashboard_row_kv_mv kv USING (log_id, table_name, run_config_name, created_at)
  WHERE ev.function IN ('is_not_in_future','is_not_in_near_future','is_data_fresh','is_older_than_n_days')
)
SELECT e.table_name, e.check_name, e.function,
       COALESCE(e.column, rm.args['column']) AS column,
       COUNT(*) AS offenders
FROM e
JOIN arg rm ON rm.check_id = e.check_id
WHERE (
    (e.function='is_not_in_future' AND to_timestamp(e.value) > (current_timestamp() + make_interval(0,0,0,0,0,0, CAST(coalesce(rm.args['offset'],'0') AS INT))))
 OR (e.function='is_not_in_near_future' AND to_timestamp(e.value) BETWEEN current_timestamp()
      AND (current_timestamp() + make_interval(0,0,0,0,0,0, CAST(coalesce(rm.args['offset'],'0') AS INT))))
 OR (e.function='is_data_fresh' AND
      (unix_timestamp(coalesce(to_timestamp(rm.args['base_timestamp']), current_timestamp()))
       - unix_timestamp(to_timestamp(e.value))) / 60.0 > CAST(coalesce(rm.args['max_age_minutes'],'0') AS DOUBLE))
 OR (e.function='is_older_than_n_days' AND datediff(coalesce(to_date(rm.args['curr_date']), current_date()), to_date(e.value)) < CAST(coalesce(rm.args['days'],'0') AS INT))
)
GROUP BY e.table_name, e.check_name, e.function, COALESCE(e.column, rm.args['column'])
ORDER BY offenders DESC;
```

> For `is_older_than_col2_for_n_days`, use `kv_pivot` to compare two columns per `log_id`.

### 7.2 Dataset Level

#### `is_unique` — duplicate key counts by table & rule

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function='is_unique'
)
SELECT ev.table_name,
       ev.check_name,
       COUNT(DISTINCT ev.log_id) AS rows_flagged,
       any_value(args['columns']) AS key_columns_json
FROM dashboard_event_base_mv ev
JOIN arg ON arg.check_id = ev.check_id
WHERE ev.function = 'is_unique'
GROUP BY ev.table_name, ev.check_name
ORDER BY rows_flagged DESC;
```

#### Aggregates: `is_aggr_*`

```sql
WITH arg AS (
  SELECT check_id, function, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv
  WHERE function IN ('is_aggr_not_greater_than','is_aggr_not_less_than','is_aggr_equal','is_aggr_not_equal')
)
SELECT ev.table_name, ev.check_name, ev.function, COUNT(DISTINCT ev.log_id) AS rows_flagged
FROM dashboard_event_base_mv ev
JOIN arg ON arg.check_id = ev.check_id
GROUP BY ev.table_name, ev.check_name, ev.function
ORDER BY rows_flagged DESC;
```

#### `sql_query` (dataset)

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='sql_query'
)
SELECT ev.table_name, ev.check_name, COUNT(DISTINCT ev.log_id) AS rows_flagged
FROM dashboard_event_base_mv ev
JOIN arg ON arg.check_id = ev.check_id
GROUP BY ev.table_name, ev.check_name
ORDER BY rows_flagged DESC;
```

#### `compare_datasets`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='compare_datasets'
)
SELECT ev.table_name, ev.check_name, COUNT(DISTINCT ev.log_id) AS rows_flagged
FROM dashboard_event_base_mv ev
JOIN arg ON arg.check_id = ev.check_id
GROUP BY ev.table_name, ev.check_name
ORDER BY rows_flagged DESC;
```

#### `is_data_fresh_per_time_window`

```sql
WITH arg AS (
  SELECT check_id, from_json(arguments_json, 'MAP<STRING,STRING>') AS args
  FROM dashboard_rule_map_mv WHERE function='is_data_fresh_per_time_window'
)
SELECT ev.table_name, ev.check_name, COUNT(*) AS windows_failed
FROM dashboard_event_base_mv ev
JOIN arg ON arg.check_id = ev.check_id
GROUP BY ev.table_name, ev.check_name
ORDER BY windows_failed DESC;
```

---

## 8. Dashboard Wiring Tips

- Starter tiles:
  - Events by day (ERROR/WARN split) from `dashboard_events_mv`.
  - Top offending checks by table (`COUNT(*) FROM dashboard_event_base_mv GROUP BY table_name, check_name`).
  - Null/empty density per row (`dashboard_row_value_stats_mv`).
  - Function filter (selector) bound to `dashboard_event_base_mv.function`.
- For deep “offender lists,” add link‑outs to a notebook that runs the function‑specific snippet with the chosen `check_id`/table filter.

---

## 9. Performance & Partitioning Notes

- If `checks_log` grows to tens of millions of rows, consider **Z‑ORDER** on `(table_name, run_config_name, created_at)` and/or clustering by `table_name`.  
- For the MVs, Databricks manages refresh/incremental maintenance; we ensure the **warehouse** is assigned and scheduled.  
- Avoid over‑partitioning by `check_id` (5K+ keys) at the base table — skew risk. Prefer **coarser** partitioning (e.g., by day, table_name) and rely on Z‑ORDER for secondary pruning.

---

## 10. Change Log

- v1.0 — Initial pack: ER map, five MVs, and function query library (row + dataset).

---

### Appendix — Quick Sanity Checks

```sql
-- 1) MV counts
SELECT 'event_base' src, COUNT(*) FROM dashboard_event_base_mv UNION ALL
SELECT 'row_kv'     , COUNT(*) FROM dashboard_row_kv_mv UNION ALL
SELECT 'row_stats'  , COUNT(*) FROM dashboard_row_value_stats_mv UNION ALL
SELECT 'rule_map'   , COUNT(*) FROM dashboard_rule_map_mv UNION ALL
SELECT 'events'     , COUNT(*) FROM dashboard_events_mv;

-- 2) Any events missing check_id?
SELECT severity, COUNT(*) AS events_no_check_id
FROM dashboard_event_base_mv
WHERE check_id IS NULL
GROUP BY severity;

-- 3) Top checks
SELECT table_name, check_name, COUNT(*) AS events
FROM dashboard_event_base_mv
GROUP BY table_name, check_name
ORDER BY events DESC
LIMIT 50;
```