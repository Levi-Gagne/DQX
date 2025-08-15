# DQX Function Coverage Matrix (v0.8.0)

**Scope:** quick reference for DQX built-ins with *fail semantics* and practical SQL patterns to mine offenders from `dq_dev.dqx.checks_log` (arrays in `_errors` + `row_snapshot`).

**Docs:** 
- Row-level: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#row-level-checks-reference
- Dataset-level: https://databrickslabs.github.io/dqx/docs/reference/quality_rules/#dataset-level-checks-reference
- Source (impl): https://github.com/databrickslabs/dqx/blob/v0.8.0/src/databricks/labs/dqx/check_funcs.py

> Notes:> - Some checks require parameters (`limit`, `allowed`, etc.). Prefer joining `checks_log.check_id[]` → `dq_dev.dqx.checks` to fetch `check.arguments.*`. If `check_id` is empty in your log, either fix the writer or parse `e.message` heuristically.> - `e.columns` may be null for dataset-wide rules. When null, either scan all `row_snapshot` KV pairs or use the rules table to scope the target columns.> - Timestamps in `row_snapshot.value` are strings; cast in SQL as needed (`to_date`, `to_timestamp`, `CAST(... AS DOUBLE)`).

### Row-level checks

| Check | Level | Official description | Key args | Fails when (opposite semantics) |
|---|---|---|---|---|
| `is_not_null` | row | Checks whether the values in the input column are not null. | column | value IS NULL |
| `is_not_empty` | row | Checks whether the values in the input column are not empty (but may be null). | column | value IS NOT NULL AND length(trim(value)) = 0 |
| `is_not_null_and_not_empty` | row | Checks whether the values in the input column are not null and not empty. | column, trim_strings(optional) | value IS NULL OR length(CASE WHEN trim_strings THEN trim(value) ELSE value END) = 0 |
| `is_in_list` | row | Checks whether the values in the input column are present in the list of allowed values (nulls allowed). | column, allowed(array) | value IS NOT NULL AND value NOT IN allowed_list |
| `is_not_null_and_is_in_list` | row | Checks whether the values in the input column are not null AND present in the allowed list. | column, allowed(array) | value IS NULL OR value NOT IN allowed_list |
| `is_not_null_and_not_empty_array` | row | Checks whether the values in the array-typed input column are not null and not empty. | column(array) | value IS NULL OR size(value) = 0 |
| `is_in_range` | row | Checks whether the values in the input column are in the provided range (inclusive). | column, min_limit, max_limit | value < min_limit OR value > max_limit |
| `is_not_in_range` | row | Checks whether the values in the input column are outside the provided range (inclusive). | column, min_limit, max_limit | value BETWEEN min_limit AND max_limit |
| `is_not_less_than` | row | Checks whether the values in the input column are not less than the provided limit. | column, limit | value < limit |
| `is_not_greater_than` | row | Checks whether the values in the input column are not greater than the provided limit. | column, limit | value > limit |
| `is_valid_date` | row | Checks whether the values in the input column have valid date formats. | column, date_format(optional) | to_date(value, fmt) IS NULL |
| `is_valid_timestamp` | row | Checks whether the values in the input column have valid timestamp formats. | column, timestamp_format(optional) | to_timestamp(value, fmt) IS NULL |
| `is_not_in_future` | row | Checks whether timestamp values are not in the future relative to current_timestamp + offset. | column, offset(seconds), curr_timestamp(optional) | value > curr_timestamp + offset |
| `is_not_in_near_future` | row | Checks whether timestamp values are not in the near future (between now and now+offset). | column, offset(seconds), curr_timestamp(optional) | current_timestamp < value < current_timestamp + offset |
| `is_older_than_n_days` | row | Checks whether values in one input column are at least N days older than the current (or provided) date. | column, days, curr_date(optional), negate(optional) | DATEDIFF(curr_date, value) < days |
| `is_older_than_col2_for_n_days` | row | Checks whether values in one column are at least N days older than values in another column. | column1, column2, days, negate(optional) | DATEDIFF(column2, column1) < days |
| `regex_match` | row | Checks whether the values in the input column match a given regex (negate to invert). | column, regex, negate(optional) | negate = false AND NOT regexp_like(value, regex); OR negate = true AND regexp_like(value, regex) |
| `is_valid_ipv4_address` | row | Checks whether the values have valid IPv4 address format. | column | value is not a valid IPv4 literal |
| `is_ipv4_address_in_cidr` | row | Checks whether IPv4 addresses fall within a given CIDR block. | column, cidr_block | value NOT IN cidr_block |
| `sql_expression` | row | Checks whether an ad‑hoc SQL boolean expression evaluates to True (fail) for a row. | expression, msg(optional), name(optional), negate(optional), columns(optional) | expression evaluates to True (or False if negate) |
| `is_data_fresh` | row | Checks whether a timestamp/date column is not older than the specified number of minutes from a base timestamp. | column, max_age_minutes, base_timestamp(optional) | base_timestamp - value > max_age_minutes |



### SQL extraction patterns — row level


**`is_not_null`**

```sql
-- List offending (NULL) values for rows flagged by 'is_not_null'
WITH e AS (
  SELECT l.log_id, l.table_name, e.name AS check_name, e.columns AS rule_cols, kv.column, kv.value
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  WHERE e.function = 'is_not_null'
)
SELECT table_name, check_name, column, COUNT(*) AS null_rows
FROM e
WHERE (rule_cols IS NULL OR array_contains(rule_cols, column))
  AND value IS NULL
GROUP BY table_name, check_name, column
ORDER BY null_rows DESC;
```


**`is_not_empty`**

```sql
-- Offending empty-string values for rows flagged by 'is_not_empty'
WITH e AS (
  SELECT l.log_id, l.table_name, e.name AS check_name, e.columns AS rule_cols, kv.column, kv.value
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  WHERE e.function = 'is_not_empty'
)
SELECT table_name, check_name, column, COUNT(*) AS empty_rows
FROM e
WHERE (rule_cols IS NULL OR array_contains(rule_cols, column))
  AND value IS NOT NULL AND length(trim(value)) = 0
GROUP BY table_name, check_name, column
ORDER BY empty_rows DESC;
```


**`is_not_null_and_not_empty`**

```sql
-- Offenders for 'is_not_null_and_not_empty'
WITH e AS (
  SELECT l.log_id, l.table_name, e.name AS check_name, e.columns AS rule_cols, e.user_metadata, kv.column, kv.value
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  WHERE e.function = 'is_not_null_and_not_empty'
)
SELECT table_name, check_name, column,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_rows,
       SUM(CASE WHEN value IS NOT NULL AND length(trim(value)) = 0 THEN 1 ELSE 0 END) AS empty_rows
FROM e
WHERE (rule_cols IS NULL OR array_contains(rule_cols, column))
GROUP BY table_name, check_name, column
ORDER BY null_rows DESC, empty_rows DESC;
```


**`is_in_list`**

```sql
-- Uses rules table to get 'allowed' list (preferred).
-- If check_id[] is empty in checks_log, consider parsing e.message, or fix the writer to populate check_id.
WITH rule_map AS (
  SELECT check_id, from_json(check.arguments.allowed, 'ARRAY<STRING>') AS allowed
  FROM dq_dev.dqx.checks
  WHERE check.function = 'is_in_list'
),
e AS (
  SELECT l.log_id, l.table_name, e.name AS check_name, e.columns AS rule_cols, kv.column, kv.value, cid AS check_id
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  LATERAL VIEW OUTER explode(check_id) c AS cid
  WHERE e.function = 'is_in_list'
)
SELECT e.table_name, e.check_name, e.column, COUNT(*) AS offenders
FROM e JOIN rule_map r ON r.check_id = e.check_id
WHERE (e.rule_cols IS NULL OR array_contains(e.rule_cols, e.column))
  AND e.value IS NOT NULL AND NOT array_contains(r.allowed, e.value)
GROUP BY e.table_name, e.check_name, e.column
ORDER BY offenders DESC;
```


**`is_not_null_and_is_in_list`**

```sql
-- Same as is_in_list, but count both NULLs and not-in-list values as offenders.
```


**`is_not_null_and_not_empty_array`**

```sql
-- Not directly derivable from row_snapshot (stringified). Use checks on the source dataset or ensure row_snapshot preserves array sizes.
```


**`is_in_range`**

```sql
-- Requires min/max from rules (join via check_id) or parse e.message.
WITH rule_map AS (
  SELECT check_id,
         check.arguments.min_limit AS min_limit,
         check.arguments.max_limit AS max_limit
  FROM dq_dev.dqx.checks WHERE check.function = 'is_in_range'
),
e AS (
  SELECT l.log_id, l.table_name, e.name AS check_name, e.columns AS rule_cols, kv.column, kv.value, cid AS check_id
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  LATERAL VIEW OUTER explode(check_id) c AS cid
  WHERE e.function = 'is_in_range'
)
SELECT e.table_name, e.check_name, e.column, COUNT(*) AS offenders
FROM e JOIN rule_map r ON r.check_id = e.check_id
WHERE (e.rule_cols IS NULL OR array_contains(e.rule_cols, e.column))
  AND (CAST(e.value AS DOUBLE) < CAST(r.min_limit AS DOUBLE)
       OR CAST(e.value AS DOUBLE) > CAST(r.max_limit AS DOUBLE))
GROUP BY e.table_name, e.check_name, e.column
ORDER BY offenders DESC;
```


**`is_not_in_range`**

```sql
-- Mirror of is_in_range with the predicate negated.
```


**`is_not_less_than`**

```sql
-- Join to checks to fetch 'limit'; predicate: CAST(value AS DOUBLE) < CAST(limit AS DOUBLE).
```


**`is_not_greater_than`**

```sql
-- Join to checks to fetch 'limit'; predicate: CAST(value AS DOUBLE) > CAST(limit AS DOUBLE).
```


**`is_valid_date`**

```sql
-- Treat values that fail date parsing as offenders
WITH e AS (
  SELECT l.table_name, e.name AS check_name, e.columns AS rule_cols, kv.column, kv.value
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  WHERE e.function = 'is_valid_date'
)
SELECT table_name, check_name, column, COUNT(*) AS bad_dates
FROM e
WHERE (rule_cols IS NULL OR array_contains(rule_cols, column))
  AND to_date(value, 'yyyy-MM-dd') IS NULL  -- adjust fmt if provided via rules
GROUP BY table_name, check_name, column
ORDER BY bad_dates DESC;
```


**`is_valid_timestamp`**

```sql
-- Similar to is_valid_date; use to_timestamp(value, 'yyyy-MM-dd HH:mm:ss') IS NULL.
```


**`is_not_in_future`**

```sql
-- Offender predicate example: to_timestamp(value) > current_timestamp() + make_interval(0,0,0,0,0,0, offset_seconds).
```


**`is_not_in_near_future`**

```sql
-- Offender predicate example: to_timestamp(value) BETWEEN current_timestamp() AND current_timestamp() + INTERVAL offset_seconds SECONDS.
```


**`is_older_than_n_days`**

```sql
-- Offender predicate: datediff(current_date(), to_date(value)) < days.
```


**`is_older_than_col2_for_n_days`**

```sql
-- Requires two columns from row_snapshot; join row_snapshot to itself pivoted by column name to compare.
```


**`regex_match`**

```sql
-- Extract regex from rules; predicate NOT regexp_like(value, regex) unless negate.
```


**`is_valid_ipv4_address`**

```sql
-- Validation typically requires a helper UDF; the log already marks offenders. Use log for counts, not recomputation.
```


**`is_ipv4_address_in_cidr`**

```sql
-- Requires IP math; prefer DQX evaluation. Use logs for offenders aggregation.
```


**`sql_expression`**

```sql
-- Free-form; prefer relying on message or check_id→rules join for parameters.
```


**`is_data_fresh`**

```sql
-- Offender predicate: (unix_timestamp(base_ts) - unix_timestamp(value)) / 60 > max_age_minutes.
```


### Dataset-level checks

| Check | Level | Official description | Key args | Fails when (opposite semantics) |
|---|---|---|---|---|
| `is_unique` | dataset | Checks whether the values in the input column(s) are unique; duplicates fail. Nulls are distinct by default. | columns, nulls_distinct(default True) | duplicate key(s) present |
| `is_aggr_not_greater_than` | dataset | Checks whether aggregated values over groups/all rows are NOT greater than the provided limit. | column(optional for count), limit, aggr_type('count'|'sum'|'avg'|'min'|'max'), group_by(optional) | aggr_value > limit |
| `is_aggr_not_less_than` | dataset | Checks whether aggregated values over groups/all rows are NOT less than the provided limit. | column(optional for count), limit, aggr_type, group_by(optional) | aggr_value < limit |
| `is_aggr_equal` | dataset | Checks whether aggregated values over groups/all rows equal the provided limit. | column(optional for count), limit, aggr_type, group_by(optional) | aggr_value != limit |
| `is_aggr_not_equal` | dataset | Checks whether aggregated values over groups/all rows are NOT equal to the provided limit. | column(optional for count), limit, aggr_type, group_by(optional) | aggr_value = limit |
| `foreign_key (aka is_in_list)` | dataset | Checks whether input columns exist in a reference DataFrame/Table; scalable alternative to row-level is_in_list. | columns, ref_columns, [ref_df_name|ref_table], negate(optional) | ref lookup misses (or hits if negate=True) |
| `sql_query` | dataset | Runs a custom SQL query that yields a boolean condition column (True=fail). Must merge back to input. | query, input_placeholder, merge_columns, condition_column, msg(optional), name(optional), negate(optional) | condition_column = True (or False if negate) |
| `compare_datasets` | dataset | Compares two DataFrames at row & column levels; can include missing-row detection. | columns, ref_columns, exclude_columns(optional), [ref_df_name|ref_table], check_missing_records(False), null-safe flags | row/column differences detected |
| `is_data_fresh_per_time_window` | dataset | Freshness check ensuring at least X records arrive within each Y-minute time window. | column(timestamp), window_minutes, min_records_per_window, lookback_windows(optional), curr_timestamp(optional) | any window has count < min_records_per_window |



### SQL extraction patterns — dataset level


**`is_unique`**

```sql
-- If check_id is populated, prefer recomputing on the source dataset.
-- From logs, you can still aggregate offenders by the key columns listed in e.columns.
WITH e AS (
  SELECT l.table_name, e.name AS check_name, e.columns AS key_cols, kv.column, kv.value
  FROM dq_dev.dqx.checks_log l
  LATERAL VIEW OUTER explode(_errors) t AS e
  LATERAL VIEW OUTER explode(row_snapshot) rs AS kv
  WHERE e.function = 'is_unique'
)
SELECT table_name, check_name, collect_set(column) AS offender_columns, COUNT(*) AS rows_flagged
FROM e
WHERE key_cols IS NULL OR array_contains(key_cols, column)
GROUP BY table_name, check_name
ORDER BY rows_flagged DESC;
```


**`is_aggr_not_greater_than`**

```sql
-- Prefer recomputing in source; logs may only summarize. Parse e.message or join to rules for parameters.
```


**`is_aggr_not_less_than`**

```sql
-- Same comment as above.
```


**`is_aggr_equal`**

```sql
-- Same comment as above.
```


**`is_aggr_not_equal`**

```sql
-- Same comment as above.
```


**`foreign_key (aka is_in_list)`**

```sql
-- From logs: list missing values per key column; for full detail, recompute using ref_table in SQL.
```


**`sql_query`**

```sql
-- Highly custom; rely on message/merge_columns from rules.
```


**`compare_datasets`**

```sql
-- From logs: count mismatches by column; for full diff, re-run comparison on sources.
```


**`is_data_fresh_per_time_window`**

```sql
-- Prefer recomputing over source data; logs can summarize windows that failed.
```



