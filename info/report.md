# CLA Data Quality Rule Guidelines for DQX

This guideline describes how to author YAML‑based data quality rules for Databricks DQX within CLA and explains the additional conventions used by the Data Quality (DQ) team. It also provides an overview of available built‑in functions and describes how to route valid and invalid records to appropriate tables via run configurations.

## 1 Required fields and ordering

At CLA every rule must include **all** of the following keys in the specific order shown below. DQX itself only requires a `criticality` and a `check`, but CLA enforces the extra fields for traceability and routing.

1. **`table_name`** – A fully qualified name (catalog.schema.table) for the dataset the rule applies to. This value is ignored by DQX but used by CLA when loading rules into a configuration table.
2. **`name`** – A short, unique identifier for the rule within the given `table_name`.
3. **`criticality`** – Either `error` or `warn`. DQX uses this flag to decide whether failing rows should be quarantined (`error`) or kept in the valid set but marked (`warn`)【669984891662410†L145-L153】.
4. **`run_config_name`** – **Mandatory at CLA.** This string ties the rule to a run configuration defined in your `dqx_output_config.yaml`. Each run configuration specifies where valid and invalid rows should be written.
5. **`check`** – A map describing the validation:
   - **`function`** – The name of a built‑in or custom check. DQX determines whether the rule is row‑level or dataset‑level based on this value.
   - **`arguments`** – (Optional) A map of parameters accepted by the function. Omit this key when the function requires no parameters.
   - **`for_each_column`** – (Optional; only for row‑level checks) a list of columns for checks that need to be applied to multiple columns individually.
6. **`filter`** – (Optional) A Spark SQL predicate restricting rows that the check applies to.
7. **`user_metadata`** – (Optional) A map of arbitrary key/value pairs. DQX attaches these tags to any row‑level errors or warnings produced by the rule.

This ordering ensures consistency across rules. Even though `arguments`, `for_each_column`, `filter` and `user_metadata` are optional, the first five fields (`table_name`, `name`, `criticality`, `run_config_name`, `check`) are required for every rule.

## 2 Run configurations (`dqx_output_config.yaml`)

The **run configuration** defines where DQX should write valid and invalid rows for a given group of rules. These definitions live in a separate YAML file. For example:

```yaml
# dqx_output_config.yaml

default:
  output_config:
    location: dq_dev.validation.dqx_log
    mode: append
    options:
      mergeSchema: true
  quarantine_config:
    location: dq_dev.validation.dqx_quarantine
    mode: append
    options:
      mergeSchema: true

none:
  output_config:
    location: none
    mode: none
    options: {}
  quarantine_config:
    location: none
    mode: none
    options: {}
```

- The `default` run configuration writes valid rows to the table `dq_dev.validation.dqx_log` and invalid rows to `dq_dev.validation.dqx_quarantine`, appending to each and merging the schema.
- The `none` run configuration suppresses output by using a `none` location and mode for both valid and invalid rows.

A `run_config_name` in your rule must match one of the top‑level keys in this file (e.g., `default` or `none`). When applying the checks, your loader code can look up the corresponding `output_config` and `quarantine_config` entries and route the results accordingly.

## 3 Template for a rule

A generic rule, not tied to a specific table, looks like this. The keys appear in the order described in Section 1. Optional fields are commented out.

```yaml
- table_name: catalog.schema.table             # fully qualified dataset name (required)
  name: descriptive_rule_name                 # unique within table_name (required)
  criticality: error | warn                   # how failures are handled【669984891662410†L145-L153】 (required)
  run_config_name: default                    # REQUIRED at CLA; must match a run_config in dqx_output_config.yaml
  check:
    function: function_name                   # name of built-in or custom check (required)
    # arguments:                             # optional; omit when not needed
    #   ...                                  # key/value pairs supported by the function
    # for_each_column:                       # optional; list of columns for per‑column checks
    #   - col1
    #   - col2
  # filter: spark_sql_predicate               # optional predicate to restrict rows
  # user_metadata:                            # optional tags for auditing
  #   key1: value1
  #   key2: value2
```

Important points:

- `table_name`, `name`, `criticality`, `run_config_name` and `check.function` are required. Without these fields the rule is considered invalid.
- `arguments` should only be included when the chosen function accepts parameters. If no parameters are needed, omit the `arguments` key entirely.
- `for_each_column` is only relevant for row‑level checks that you want to apply to multiple columns individually.
- `filter` and `user_metadata` are optional and can be omitted entirely.

## 4 Built‑in DQX functions

The table below summarizes the built‑in functions provided by DQX, whether they operate at row or dataset scope, the required and optional arguments, and an example of how to supply the arguments. The descriptions below are based on the official DQX documentation【669984891662410†L30-L131】【669984891662410†L960-L1013】.

| Function | Scope | Required arguments | Optional arguments | Example arguments |
| --- | --- | --- | --- | --- |
| `is_not_null` | Row | `column` | — | `column: email`【669984891662410†L38-L44】 |
| `is_not_empty` | Row | `column` | — | `column: type_name`【669984891662410†L38-L44】 |
| `is_not_null_and_not_empty` | Row | `column` | `trim_strings` (boolean) | `column: type_name`, `trim_strings: true`【669984891662410†L45-L48】 |
| `is_in_list` | Row | `column`, `allowed` (list) | — | `column: status`, `allowed: [Active, Closed]`【669984891662410†L49-L53】 |
| `is_not_null_and_is_in_list` | Row | `column`, `allowed` (list) | — | `column: status`, `allowed: [A, B, C]`【669984891662410†L54-L58】 |
| `is_not_null_and_not_empty_array` | Row | `column` | — | `column: tags_array`【669984891662410†L59-L61】 |
| `is_in_range` | Row | `column`, `min_limit`, `max_limit` | — | `column: discount`, `min_limit: 0`, `max_limit: 50`【669984891662410†L62-L66】 |
| `is_not_in_range` | Row | `column`, `min_limit`, `max_limit` | — | `column: temperature`, `min_limit: 10`, `max_limit: 20`【669984891662410†L67-L71】 |
| `is_not_less_than` | Row | `column`, `limit` | — | `column: order_amount`, `limit: 1`【669984891662410†L72-L75】 |
| `is_not_greater_than` | Row | `column`, `limit` | — | `column: age`, `limit: 120`【669984891662410†L76-L79】 |
| `is_valid_date` | Row | `column` | `date_format` | `column: signup_date`, `date_format: yyyy-MM-dd`【669984891662410†L80-L83】 |
| `is_valid_timestamp` | Row | `column` | `timestamp_format` | `column: updated_ts`, `timestamp_format: yyyy-MM-dd HH:mm:ss`【669984891662410†L83-L86】 |
| `is_not_in_future` | Row | `column`, `offset` | `curr_timestamp` | `column: shipping_ts`, `offset: 86400`, `curr_timestamp: '2025-01-01T00:00:00'`【669984891662410†L87-L92】 |
| `is_not_in_near_future` | Row | `column`, `offset` | `curr_timestamp` | `column: deadline_ts`, `offset: 3600`【669984891662410†L93-L98】 |
| `is_older_than_n_days` | Row | `column`, `days` | `curr_date`, `negate` | `column: order_date`, `days: 30`, `curr_date: '2025-05-01'`, `negate: false`【669984891662410†L99-L103】 |
| `is_older_than_col2_for_n_days` | Row | `column1`, `column2`, `days` | `negate` | `column1: ship_date`, `column2: order_date`, `days: 3`【669984891662410†L104-L108】 |
| `regex_match` | Row | `column`, `regex` | `negate` | `column: email`, `regex: '^(.+)@(.+)$'`, `negate: false`【669984891662410†L109-L112】 |
| `is_valid_ipv4_address` | Row | `column` | — | `column: ip_addr`【669984891662410†L113-L115】 |
| `is_ipv4_address_in_cidr` | Row | `column`, `cidr_block` | — | `column: ip_addr`, `cidr_block: '192.168.1.0/24'`【669984891662410†L116-L119】 |
| `sql_expression` | Row | `expression` | `msg`, `columns` (list), `name`, `negate` | `expression: "status = 'Closed'"`, `msg: "Status must be closed"`, `negate: false`【669984891662410†L120-L131】 |
| `is_unique` | Dataset | `columns` (list) | `nulls_distinct` (boolean) | `columns: [customer_id]`, `nulls_distinct: false`【669984891662410†L960-L967】 |
| `is_aggr_not_greater_than` | Dataset | `limit` | `column` (optional for count), `aggr_type`, `group_by` | `column: quantity`, `aggr_type: count`, `limit: 100000`【669984891662410†L969-L975】 |
| `is_aggr_not_less_than` | Dataset | `limit` | `column` (optional for count), `aggr_type`, `group_by` | `column: revenue`, `aggr_type: sum`, `group_by: [region]`, `limit: 1000`【669984891662410†L976-L982】 |
| `is_aggr_equal` | Dataset | `limit` | `column` (optional for count), `aggr_type`, `group_by` | `column: cost`, `aggr_type: avg`, `group_by: [category]`, `limit: 50`【669984891662410†L983-L989】 |
| `is_aggr_not_equal` | Dataset | `limit` | `column` (optional for count), `aggr_type`, `group_by` | `column: rating`, `aggr_type: max`, `limit: 5`【669984891662410†L990-L996】 |
| `foreign_key` | Dataset | `columns` (list), `ref_columns` (list) | One of `ref_table` or `ref_df_name`, `negate` | `columns: [customer_id]`, `ref_columns: [id]`, `ref_table: dq_dev.master.customers`, `negate: false`【669984891662410†L997-L1013】 |
| `sql_query` | Dataset | `query`, `merge_columns` (list), `condition_column` | `input_placeholder`, `msg`, `name`, `negate` | `query: "SELECT key, SUM(value)=0 AS condition FROM {{ input_view }} GROUP BY key"`, `merge_columns: [key]`, `condition_column: condition`, `input_placeholder: input_view`, `msg: "Sum must be > 0"`, `negate: false`【669984891662410†L1014-L1033】 |
| `compare_datasets` | Dataset | `columns` (list), `ref_columns` (list) | One of `ref_table` or `ref_df_name`; `exclude_columns` (list); `check_missing_records` (boolean) | `columns: [customer_id]`, `ref_columns: [ref_customer_id]`, `ref_table: dq_dev.archive.customers_snapshot`, `exclude_columns: [status]`, `check_missing_records: true`【669984891662410†L1034-L1059】 |

**Notes:**

- Row‑level checks operate on individual rows and may optionally be applied to multiple columns via `for_each_column`.
- Dataset‑level checks operate on aggregated values across one or more rows and return a result for each input row【669984891662410†L943-L952】.
- When using dataset‑level functions that reference external tables (e.g., `foreign_key` or `compare_datasets`), you must supply either `ref_table` or a named DataFrame via `ref_df_name`, but not both【669984891662410†L997-L1013】【669984891662410†L1034-L1059】.

## 5 Example dataset and sample rules

The following example combines a simple dataset with a set of rules that demonstrate common data quality checks and the required YAML structure.

### Example dataset

| customer_id | email             | status    | signup_date |
|-----------:|------------------|-----------|-------------|
| 1          | alice@example.com | Active    | 2025-07-01 |
| 2          | bob@example.com   | Closed    | 2025-07-02 |
| 3          | charlie@invalid   | Active    | 2025-08-15 |
| 4          | null              | Suspended | 2025-07-20 |
| 5          | eve@example.com   | null      | 2025-06-25 |
| 6          | (empty string)    | Active    | (empty)    |

This tiny dataset is enough to demonstrate common data quality checks: unique identifiers, non‑null fields, allowed values, regex patterns, and date validity.

### Sample rules for the `customers` table

Each rule below uses `run_config_name: default` to write results to the `default` output and quarantine tables defined in `dqx_output_config.yaml`. Only the required fields and necessary arguments are included.

```yaml
# Ensure customer_id is unique across all rows
- table_name: dq_dev.demo.customers
  name: customer_id_unique
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments:
      columns: [customer_id]

# Ensure email column is not null
- table_name: dq_dev.demo.customers
  name: email_not_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    arguments:
      column: email

# Validate email format (simple pattern)
- table_name: dq_dev.demo.customers
  name: email_format_valid
  criticality: warn
  run_config_name: default
  check:
    function: regex_match
    arguments:
      column: email
      regex: '^(.+)@(.+)$'
      negate: false

# Restrict status values to a defined list
- table_name: dq_dev.demo.customers
  name: status_allowed_values
  criticality: warn
  run_config_name: default
  check:
    function: is_in_list
    arguments:
      column: status
      allowed: [Active, Closed]

# Check that signup_date is a valid date
- table_name: dq_dev.demo.customers
  name: signup_date_valid
  criticality: warn
  run_config_name: default
  check:
    function: is_valid_date
    arguments:
      column: signup_date
      date_format: yyyy-MM-dd
```

These rules demonstrate how to use different built‑in functions with minimal fields. You can add `filter` conditions or `user_metadata` tags as needed. For example, to run a check only on active customers you could include `filter: status = 'Active'`.

## 6 Routing valid and invalid rows

DQX rules do not include destinations for valid or invalid rows. CLA accomplishes routing in two ways:

1. **Run configurations** – Use the `run_config_name` in your YAML to reference an entry in `dqx_output_config.yaml`. When executing the rules, your pipeline should read this file and write valid rows to `output_config.location` and invalid rows to `quarantine_config.location` using the specified write `mode` and `options`.

2. **Additional fields in a configuration table** – Alternatively, your Delta configuration table can include extra columns, such as `valid_target_table` and `quarantine_target_table`. These are not part of the YAML but exist in the Delta table and can be retrieved by your loader. For example:

```yaml
# Example rule entry with additional metadata (stored in the Delta table, not in YAML)
table_name: dq_dev.demo.customers
name: email_not_null
criticality: error
run_config_name: default
check:
  function: is_not_null
  arguments:
    column: email
valid_target_table: dq_dev.validation.dqx_log         # not part of the YAML spec
quarantine_target_table: dq_dev.validation.dqx_quarantine # not part of the YAML spec
```

Including these fields in the table allows your processing script to determine where to write valid and invalid data without hardcoding targets in the rule definitions.

---

This document should provide enough detail to create CLA‑compliant DQX rules, understand the purpose of each key, select the appropriate built‑in functions, and route the results to the desired tables. Always ensure that every rule includes a `run_config_name` that matches an entry in your `dqx_output_config.yaml` so that the processing engine knows where to send valid and invalid rows.
