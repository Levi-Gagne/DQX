# CLA DQX Check Creation Guidelines (v0.8.x)

Purpose: how we author fast, reliable YAML rules for Databricks DQX at CLA. This is the working reference. Keep it blunt, useful, and team-facing.

---

## TL;DR

- Rules live in YAML. We load them, hash a **canonical payload** to `check_id`, and write to `dq_*.*.checks`.  
- Required key order: `table_name`, `name`, `criticality`, `run_config_name`, `check{function,for_each_column?,arguments?}`, `filter?`, `user_metadata?`.  
- Prefer **row-level** checks + `for_each_column` over dataset scans when possible. Filter aggressively.  
- Avoid rules that hit ~100% of rows. If every rule hits every row, our logs table balloons (we save the whole row snapshot).  
- Comment SQL companions + English intent under each rule (as `#` comments). It speeds reviews and audits.

---

## 1) Required fields & ordering (CLA standard)

We require these keys **in this order** for every rule:

1. `table_name` — fully qualified `catalog.schema.table`
2. `name` — lower_snake_case; unique **within table**
3. `criticality` — `error` or `warn`
4. `run_config_name` — must exist in our run‑config YAML
5. `check` — the validation block
6. `filter` — *(optional)* Spark SQL predicate that reduces scanned rows
7. `user_metadata` — *(optional)* free‑form tags (owner, channel, playbook, etc.)

> Even if DQX doesn’t enforce all of this, we do. It keeps routing and traceability clean.

---

## 2) Base template (all knobs, empty args)

This is a **skeleton** rule that lists all fields you might use. Fill only the ones your function needs.

```yaml
# --- TEMPLATE: copy/paste and fill ---
- table_name: <catalog.schema.table>
  name: <descriptive_rule_name>
  criticality: error | warn
  run_config_name: default
  check:
    function: <function_name>
    # for_each_column: [col1, col2]     # row-level: run the same rule per column
    # arguments:
    #   column: <col_name>              # row-level functions
    #   allowed: [A, B, C]              # is_in_list
    #   min_limit: <num>                # is_in_range
    #   max_limit: <num>                # is_in_range
    #   limit: <num>                    # is_not_less_than / is_not_greater_than
    #   regex: <pattern>                # regex_match
    #   expression: "<spark_sql_pred>"  # sql_expression (violations when FALSE)
    #   columns: [c1, c2, ...]          # is_unique / dataset funcs
    #   query: |                        # sql_query (dataset)
    #     SELECT k, <bool> AS condition FROM {{ input_view }}
    #   merge_columns: [k]              # sql_query join key
    #   condition_column: condition     # sql_query boolean column name
    #   input_placeholder: input_view   # sql_query input view alias (optional)
  # filter: <spark_sql_predicate>       # optional row filter
  # user_metadata:
  #   owner: data-quality
  #   reviewer: levi.gagne@claconnect.com
  #   alert_channel: ops
```

---

## 3) Full example (realistic, varied)

```yaml
# created_by: LMG
# created_at: 2025-08-15
- table_name: dq_prd.monitoring.job_run_audit
  name: terminated_runs_have_end_time
  criticality: error
  run_config_name: default
  filter: "state = 'TERMINATED'"
  check:
    function: sql_expression
    arguments:
      expression: "end_time IS NOT NULL"
  user_metadata:
    owner: dq-team
    alert_channel: teams://dq-alerts
    comment: "State TERMINATED implies end_time present"
  # WHY: ensure lifecycle completeness for terminated runs.
  # SQL (violations under filter):
  # SELECT COUNT(*) AS violation_count
  # FROM dq_prd.monitoring.job_run_audit
  # WHERE state='TERMINATED' AND end_time IS NULL;
```

---

## 4) Run configurations (routing & write mode)

Rules are grouped by `run_config_name`. Each run‑config controls log/quarantine sinks.

```yaml
# dqx_output_config.yaml
default:
  output_config:
    location: dq_dev.dqx.checks_log
    mode: append
    options: { mergeSchema: true }
  quarantine_config:
    location: dq_dev.dqx.checks_quarantine
    mode: append
    options: { mergeSchema: true }
```

**Constraints**

- `run_config_name` in each rule **must** exist here.
- Use **append** unless we’re doing a controlled backfill.
- Keep `mergeSchema: true` when we add fields.

---

## 5) Naming & style

- **Name**: lower_snake_case, start with the assertion (e.g., `run_id_is_unique`).
- **One assertion per rule.** Split composites.
- **Filters must prune** (target <~20–40% of rows when possible). `filter: 1=1` is a smell.
- **Arguments**: only set what the function consumes.
- **for_each_column**: use when the logic is identical across columns (saves config & execution planning).

---

## 6) How the loader maps YAML → `checks` (what becomes identity)

From our loader notebook:

- We build a **canonical payload** JSON: `{ table_name↓, filter(normalized), check{ function, for_each_column(sorted)?, arguments(stringified, keys sorted)? } }`  
- `check_id = sha256(payload)`
- **Not** part of identity: `name`, `criticality`, `run_config_name`, `user_metadata`.
- We **overwrite** the target table on each load, write comments, and declare an informational PK on `check_id`. We hard‑fail if duplicates exist.

> Why this matters: renaming a rule or changing criticality does **not** change identity. Changing `filter` or `arguments` does.

---

## 7) Examples by function (one per pattern)

Each example includes an optional filter, a different function, and a short SQL companion. Use these as base patterns.

### 7.1 Row‑level

**A) Presence across many columns (fast, scalable)**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: business_fields_not_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    for_each_column: [job_name, job_id, run_id, start_time, start_date, state, result_state]
  # SQL: SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit WHERE job_name IS NULL; -- repeat per column
```

**B) Allowed values with row filter**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: result_state_allowed_when_terminated
  criticality: error
  run_config_name: default
  filter: "state = 'TERMINATED'"
  check:
    function: is_in_list
    arguments:
      column: result_state
      allowed: ["SUCCESS","FAILED","CANCELED","INTERNAL_ERROR"]
  # SQL: SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit WHERE state='TERMINATED' AND result_state NOT IN (...);
```

**C) Numeric range**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: start_time_raw_is_in_valid_range
  criticality: error
  run_config_name: default
  check:
    function: is_in_range
    arguments: { column: start_time_raw, min_limit: 1, max_limit: 9223372036854775807 }
  # SQL: SELECT COUNT(*) FROM ... WHERE start_time_raw IS NULL OR start_time_raw < 1 OR start_time_raw > 9223372036854775807;
```

**D) Regex guard**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: run_page_url_is_https
  criticality: error
  run_config_name: default
  check:
    function: regex_match
    arguments: { column: run_page_url, regex: "^https://.+" }
  # SQL: SELECT COUNT(*) FROM ... WHERE run_page_url IS NOT NULL AND run_page_url NOT RLIKE '^https://.+';
```

**E) Arbitrary predicate**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: end_after_or_equal_start
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments: { expression: "end_time IS NULL OR end_time >= start_time" }
  # SQL: SELECT COUNT(*) FROM ... WHERE end_time IS NOT NULL AND end_time < start_time;
```

### 7.2 Dataset‑level

**F) Composite uniqueness**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: unique_job_id_start_time
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments: { columns: [job_id, start_time] }
  # SQL: window-count > 1 per (job_id,start_time)
```

**G) Grouped business rule via `sql_query`**

```yaml
- table_name: dq_prd.monitoring.job_run_audit
  name: job_name_maps_to_single_job_id
  criticality: error
  run_config_name: default
  check:
    function: sql_query
    arguments:
      query: |
        SELECT job_name AS k,
               (COUNT(DISTINCT job_id) = 1) AS condition
        FROM {{ input_view }}
        WHERE job_name IS NOT NULL
        GROUP BY job_name
      merge_columns: [k]
      condition_column: condition
  # SQL: HAVING MIN(job_id) <> MAX(job_id)
```

**H) Freshness by window**

```yaml
- table_name: dq_prd.monitoring.job_events
  name: events_per_window_meet_minimum
  criticality: warn
  run_config_name: default
  check:
    function: is_data_fresh_per_time_window
    arguments:
      column: event_ts
      window_minutes: 60
      min_records_per_window: 10
      lookback_windows: 24
  # SQL: per 60-min window, COUNT(*) >= 10 over last 24 windows
```

**I) Foreign key style (reference list)**

```yaml
- table_name: dq_prd.txn.fact_orders
  name: customer_id_in_dim
  criticality: error
  run_config_name: default
  check:
    function: foreign_key
    arguments:
      columns: [customer_id]
      ref_table: de_prd.dim.customers
      ref_columns: [customer_id]
  # NOTE: prefer same-cluster joins / broadcastable dims.
```

**J) Complex join rule (use sparingly)**

```yaml
- table_name: dq_prd.fin.posted_ledger
  name: orphan_ledger_entries
  criticality: error
  run_config_name: default
  check:
    function: sql_query
    arguments:
      query: |
        SELECT l.ledger_id AS k, (d.customer_id IS NOT NULL) AS condition
        FROM {{ input_view }} l
        LEFT JOIN de_prd.dim.customers d
          ON l.customer_id = d.customer_id
      merge_columns: [k]
      condition_column: condition
  # TIP: if the join side is huge, you're creating a shuffle storm. Prefer foreign_key when possible.
```

---

## 8) Authoring workflow (quick)

1. Start from the **template**. Fill only the needed arguments.  
2. Add a **filter** if you can meaningfully prune.  
3. Add a short **WHY** comment + **SQL companion** (count + inspect).  
4. Add **user_metadata** (owner, alert channel, GitHub PR link, etc.).  
5. Run loader in **validate_only** first.  
6. PR with sample counts and expected effect. Start as `warn` for risky rules; promote later.

---

## 9) Performance & pitfalls (read this before you ship)

The short version: minimize scans, minimize shuffles, minimize explosion in logs.

### 9.1 Logs blow‑up (row_snapshot duplication)

We save the **whole offending row** in `row_snapshot`. If *every* rule hits *every* row, you’ve essentially duplicated the base table into the logs table — **per rule per run**. Until we split storage (future: “log rows by table” and “log rows by rule”), assume the cost is real.

**Avoid** rules that are effectively `TRUE` for most rows. Use filters. Tighten predicates.

### 9.2 Dataset‑level vs row‑level for null checks

**Bad (slow):** one dataset‑level rule scanning the whole table to find any NULLs across many columns.  
**Better (fast):** one row‑level rule with `for_each_column` listing the columns to check for NULL.

```yaml
# BAD: scans the dataset and may group/shuffle
- table_name: de_prd.foo.bar
  name: any_nulls_bad
  criticality: error
  run_config_name: default
  check:
    function: sql_query
    arguments:
      query: |
        SELECT 1 AS k, (COUNT(*) FILTER (WHERE col1 IS NULL OR col2 IS NULL) = 0) AS condition
        FROM {{ input_view }}
      merge_columns: [k]
      condition_column: condition

# BETTER: vectorized row-level checks, no global shuffle
- table_name: de_prd.foo.bar
  name: business_cols_not_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    for_each_column: [col1, col2]
```

### 9.3 Filter early, filter often

Always add a filter when the rule only matters for a subset.

```yaml
# Good: limits to terminated runs
filter: "state = 'TERMINATED'"
```

Filtering cuts I/O and row_snapshot volume; it also reduces shuffle sizes when a check aggregates.

### 9.4 Shuffle traps

- `sql_query` with **big joins** or high‑cardinality `GROUP BY` → large shuffle and long tails. Prefer `foreign_key` or keep joins on broadcastable dims.  
- `is_unique` on high‑cardinality concatenations is fine, but don’t add extra derived keys unnecessarily.  
- Regex on huge TEXT fields across the table is expensive — normalize upstream (lower/trim) and keep patterns simple.

### 9.5 Anti‑patterns (don’t PR these)

- `filter: 1=1` or filters that still touch ~100% of rows.  
- Rules that replicate the same logic with tiny differences — use `for_each_column`.  
- `sql_query` used where a built‑in exists (`is_unique`, `is_in_list`, `is_in_range`).  
- Argument keys that the function ignores (noise).  
- Names not in lower_snake_case or that don’t state the assertion.

---

## 10) Auditing helpers (comment these under rules)

- **WHY** (plain English).  
- **SQL companion** (violation count + sample inspector).  
- **Owner** in `user_metadata`. Optional: add your email for quick pings.  
- Link to PR or runbook if relevant.

---

## 11) Versioning & change management

- Rule files are code. PRs only.  
- Include expected violation counts in the PR description.  
- Start `criticality: warn` for new high‑blast‑radius rules; promote to `error` after a stable period.  
- Loader overwrites `checks`. Don’t hand‑edit the table; it’ll be reverted on next run.

---

## 12) Appendix — sanity checks (SQL)

```sql
-- Rules table state
SELECT COUNT(*) AS rules, COUNT(DISTINCT check_id) AS unique_rules FROM dq_dev.dqx.checks;

-- Identity duplicates (should return 0 rows)
SELECT check_id, COUNT(*) c FROM dq_dev.dqx.checks GROUP BY check_id HAVING COUNT(*)>1;

-- Coverage glance
SELECT name, check.function, check.arguments FROM dq_dev.dqx.checks LIMIT 20;
```
