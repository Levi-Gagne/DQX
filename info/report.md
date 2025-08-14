# CLA Data Quality Rule Guidelines for DQX

**Purpose:** How to author high‑quality YAML rules for Databricks DQX at CLA. This is the reference engineers will use when adding or reviewing rules. Keep rules targeted (no “matches all rows”), reproducible, and fast.

---

## 1) Required fields & ordering (CLA standard)

Every rule **must** include these keys **in this order**:

1. `table_name` — fully qualified `catalog.schema.table`
2. `name` — lower_snake_case; unique **within table**
3. `criticality` — `error` or `warn`
4. `run_config_name` — must exist in your run‑config YAML
5. `check` — the validation (function, arguments, for_each_column)
6. `filter` — *(optional)* Spark SQL predicate to limit rows
7. `user_metadata` — *(optional)* free‑form tags (owner, channel, playbook, etc.)

> Even if DQX doesn’t require all of these, CLA does for traceability and routing.

**Template**

```yaml
- table_name: <catalog.schema.table>
  name: <descriptive_rule_name>
  criticality: error | warn
  run_config_name: default
  check:
    function: <function_name>
    # arguments: {...}             # include only if the function needs them
    # for_each_column: [col1, col2] # row-level only; applies the check per column
  # filter: <spark_sql_predicate>   # optional
  # user_metadata:
  #   owner: data-platform
  #   alert_channel: ops
```

---

## 2) Run configurations (routing & write mode)

Rules are grouped by `run_config_name`. Each run‑config defines where outputs go and how they’re written.

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

**Requirements**

- `run_config_name` in each rule **must** exist in this file.
- Use **append** unless you have a controlled backfill job. Overwrites require explicit agreement.
- Prefer `mergeSchema: true` when adding fields to rule outputs.

---

## 3) Naming & style

- **Rule names:** lower_snake_case; start with what’s enforced (e.g., `run_id_is_unique`, `status_in_allowed_set`).
- **One assertion per rule.** If you need multiple, create multiple rules.
- **Filters** must meaningfully reduce scope (avoid filters that match ~100%). Example:
  - Good: `filter: state = 'TERMINATED'`
  - Bad: `filter: 1 = 1`
- **Arguments**: include only when the function needs them.
- **for_each_column**: only for row‑level checks across many columns.

---

## 4) Function quick reference (most used)

| Function | Scope | Required args | Notes / examples |
|---|---|---|---|
| `is_not_null` | Row | `column` | `column: run_id` |
| `is_not_null_and_not_empty` | Row | `column` | Trim/empty check for strings. |
| `is_in_list` | Row | `column`, `allowed` | `allowed: ['Active','Closed']` |
| `is_in_range` | Row | `column`, (`min_limit`/`max_limit`) | Numeric bounds (inclusive). |
| `regex_match` | Row | `column`, `regex` | Use raw strings for backslashes in YAML. |
| `sql_expression` | Row | `expression` | Any Spark SQL predicate; returns **violations** when false. |
| `is_unique` | Dataset | `columns` | Composite unique keys supported. |

> Dataset‑level checks (`is_unique`, aggregations) evaluate across the whole dataset and then annotate rows that violate the condition.

---

## 5) SQL companion pattern (must be included as comments)

Add a short SQL block showing how to count/inspect **violations**. This makes rules auditable and reviewable.

**Patterns**

- **is_unique** on `[k1,k2]` (count violating rows across duplicate groups, exclude NULL keys):  
  ```sql
  -- violation count
  SELECT COUNT(*) AS violation_count
  FROM (
    SELECT k1,k2, COUNT(*) OVER (PARTITION BY k1,k2) AS kcnt
    FROM <table>
  ) t
  WHERE k1 IS NOT NULL AND k2 IS NOT NULL AND kcnt > 1;
  -- inspect
  SELECT *
  FROM (
    SELECT t.*, COUNT(*) OVER (PARTITION BY k1,k2) AS kcnt
    FROM <table> t
  ) x
  WHERE k1 IS NOT NULL AND k2 IS NOT NULL AND kcnt > 1;
  ```

- **sql_expression** `expr`:  
  ```sql
  -- violation count
  SELECT COUNT(*) AS violation_count
  FROM <table>
  WHERE NOT (<expr>);
  -- inspect
  SELECT * FROM <table> WHERE NOT (<expr>);
  ```

Include these under each rule as `# SQL ...` comments.

---

## 6) Examples

### A) `dq_prd.monitoring.job_run_audit` (high‑signal rules)

> Assumptions: `run_id` is PK, table partitioned by `start_date`. Only `updated_by`/`updated_at` may be NULL.

```yaml
# run_id must exist
- table_name: dq_prd.monitoring.job_run_audit
  name: run_id_is_not_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    arguments: { column: run_id }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit WHERE run_id IS NULL;

# run_id must be unique
- table_name: dq_prd.monitoring.job_run_audit
  name: run_id_is_unique
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments: { columns: [run_id] }
  # SQL:
  # SELECT COUNT(*) FROM (
  #   SELECT run_id, COUNT(*) OVER (PARTITION BY run_id) AS kcnt
  #   FROM dq_prd.monitoring.job_run_audit
  # ) t WHERE run_id IS NOT NULL AND kcnt > 1;

# start_time must not be in the future (UTC now)
- table_name: dq_prd.monitoring.job_run_audit
  name: start_time_not_in_future
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments: { expression: "start_time <= current_timestamp()" }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit WHERE start_time > current_timestamp();

# end_time >= start_time (or end_time is NULL for in-flight)
- table_name: dq_prd.monitoring.job_run_audit
  name: end_after_or_equal_start
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments: { expression: "end_time IS NULL OR end_time >= start_time" }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit
  # WHERE end_time IS NOT NULL AND end_time < start_time;

# URL must be https
- table_name: dq_prd.monitoring.job_run_audit
  name: run_page_url_is_https
  criticality: error
  run_config_name: default
  check:
    function: regex_match
    arguments: { column: run_page_url, regex: "^https://.+" }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit
  # WHERE run_page_url IS NOT NULL AND NOT (run_page_url RLIKE '^https://.+');

# FAILED runs must carry a non-empty error_code
- table_name: dq_prd.monitoring.job_run_audit
  name: failed_runs_have_error_code
  criticality: error
  run_config_name: default
  filter: "result_state = 'FAILED'"
  check:
    function: sql_expression
    arguments: { expression: "error_code IS NOT NULL AND trim(error_code) <> ''" }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit
  # WHERE result_state = 'FAILED' AND (error_code IS NULL OR trim(error_code) = '');

# Only updated_* may be NULL (others must be present)
- table_name: dq_prd.monitoring.job_run_audit
  name: required_fields_not_null
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments:
      expression: "run_id IS NOT NULL AND job_id IS NOT NULL AND job_name IS NOT NULL AND start_time IS NOT NULL"
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit
  # WHERE run_id IS NULL OR job_id IS NULL OR job_name IS NULL OR start_time IS NULL;

# job_name -> job_id consistency (same name uses one id)
- table_name: dq_prd.monitoring.job_run_audit
  name: job_name_maps_to_single_job_id
  criticality: warn
  run_config_name: default
  check:
    function: sql_query
    arguments:
      query: |
        SELECT job_name AS k, (COUNT(DISTINCT job_id) = 1) AS condition
        FROM {{ input_view }}
        WHERE job_name IS NOT NULL
        GROUP BY job_name
      merge_columns: [k]
      condition_column: condition
      input_placeholder: input_view
  # SQL:
  # SELECT COUNT(*) FROM (
  #   SELECT job_name, COUNT(DISTINCT job_id) AS dids
  #   FROM dq_prd.monitoring.job_run_audit
  #   WHERE job_name IS NOT NULL GROUP BY job_name
  # ) t WHERE dids > 1;

# Partition hygiene: start_date = CAST(start_time AS DATE)
- table_name: dq_prd.monitoring.job_run_audit
  name: start_date_matches_start_time
  criticality: warn
  run_config_name: default
  check:
    function: sql_expression
    arguments: { expression: "start_date = CAST(start_time AS DATE)" }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit
  # WHERE start_date <> CAST(start_time AS DATE);

# start_time_raw positive when present
- table_name: dq_prd.monitoring.job_run_audit
  name: start_time_raw_is_positive
  criticality: error
  run_config_name: default
  check:
    function: is_in_range
    arguments: { column: start_time_raw, min_limit: 1 }
  # SQL:
  # SELECT COUNT(*) FROM dq_prd.monitoring.job_run_audit WHERE start_time_raw IS NOT NULL AND start_time_raw < 1;
```

### B) `de_prd.gold.wkdy_dim_project` (with SQL companions)

```yaml
# project_key unique
- table_name: de_prd.gold.wkdy_dim_project
  name: project_key_is_unique
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments: { columns: [project_key] }
  # SQL:
  # SELECT COUNT(*) FROM (
  #   SELECT project_key, COUNT(*) OVER (PARTITION BY project_key) AS kcnt
  #   FROM de_prd.gold.wkdy_dim_project
  # ) t WHERE project_key IS NOT NULL AND kcnt > 1;

# project_type_name is present and not 'Unknown'
- table_name: de_prd.gold.wkdy_dim_project
  name: project_type_is_not_null_or_unknown
  criticality: error
  run_config_name: default
  check:
    function: sql_expression
    arguments: { expression: "project_type_name IS NOT NULL AND project_type_name <> 'Unknown'" }
  # SQL:
  # SELECT COUNT(*) FROM de_prd.gold.wkdy_dim_project
  # WHERE project_type_name IS NULL OR project_type_name = 'Unknown';
```

---

## 7) Review checklist (for PRs)

- [ ] Keys in required order; names in lower_snake_case.
- [ ] `run_config_name` exists in run‑config YAML.
- [ ] Each rule asserts **one** thing; arguments present only if needed.
- [ ] Filters, if present, reduce scope meaningfully.
- [ ] SQL companion (violation count + inspect) included as comments.
- [ ] No rule that trivially matches all rows.
- [ ] Dataset‑level checks don’t rely on non-deterministic expressions.
- [ ] New rules won’t explode shuffle size (check distinct/group‑by cardinality).

---

## 8) Performance tips

- Avoid wide `sql_query` scans over huge joins; prefer keys within the same table when possible.
- Use `is_unique` vs. ad‑hoc SQL for uniqueness—engine optimizations apply.
- Keep regexes simple; pre‑normalize text (lower/trim) in the expression instead of using advanced patterns when possible.

---

## 9) Versioning & change management

- Treat rule files as code: PR review required.
- Include **why this rule** in the PR description plus sample violation counts from SQL companion.
- Use `warn` first for new, potentially high‑impact rules; promote to `error` after monitoring.

---

## 10) Common anti‑patterns

- `filter: 1 = 1` or filters that still match ~100% of rows.
- Duplicating the same assertion with tiny variations—prefer one well‑scoped rule.
- Over‑using `sql_query` where a built‑in (`is_unique`, `is_in_list`, `is_in_range`) suffices.
- Adding `arguments` that the function ignores.

---

**Questions or updates?** Ping the Data Platform team in #data-quality.