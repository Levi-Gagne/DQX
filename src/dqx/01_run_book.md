# Runbook — `01_load_dqx_checks` (DQX Rule Loader)

**Purpose**  
Load and validate DQX rules defined in YAML, canonicalize them into a stable `check_id`, and overwrite the target **rules catalog** table with helpful metadata, comments, and a PK declaration. Designed for Databricks + Unity Catalog.

---

## Quick Start

**Standard run**
```python
main()
```

**Dry run (no write)**
```python
main(dry_run=True)
```

**Validate only (no write)**
```python
main(validate_only=True)
```

**Common overrides**
```python
main(
  output_config_path="resources/dqx_config.yaml",
  rules_dir=None,                          # else pulled from config
  batch_dedupe_mode="warn",                # warn | error | skip
  time_zone="America/Chicago"
)
```

---

## Inputs & Outputs

**Config (`resources/dqx_config.yaml`)**
- `dqx_yaml_checks`: folder with YAML files (recursive; supports `dbfs:/` via `/dbfs/...` bridge)
- `dqx_checks_config_table_name`: fully qualified UC table to overwrite, e.g. `dq_dev.dqx.checks`

**YAML rule shape (per rule)**
- Required: `table_name` (catalog.schema.table), `name`, `criticality` (`warn|warning|error`), `run_config_name`, `check.function`
- Optional: `check.for_each_column` (list of strings), `check.arguments` (map; values stringified), `filter`, `user_metadata`, `active` (default `true`)

**Output table (overwrite)**
- Schema includes: `check_id` (PRIMARY KEY by design), `check_id_payload`, `table_name`, `name`, `criticality`, `check{function,for_each_column,arguments}`, `filter`, `run_config_name`, `user_metadata`, `yaml_path`, `active`, `created_by/at`, `updated_by/at`.
- **Metadata**: table & column **comments** are applied.
- **Constraint**: attempts `ALTER TABLE ... ADD CONSTRAINT pk_check_id PRIMARY KEY (check_id) RELY` (Unity Catalog).
- **Guardrail**: hard **runtime assertion** ensures `check_id` uniqueness (fails the run if duplicates exist).

---

## What the Notebook Does (High Level)

1) **Environment banner** — prints cluster/warehouse info with local timezone.  
2) **Read config** — resolves `rules_dir` and `delta_table_name`.  
3) **Discover YAMLs** — recursively finds `*.yaml|*.yml` under `rules_dir` (including nested folders).  
4) **Validate (optional short-circuit)** — per file + per rule + **DQEngine.validate_checks**.  
5) **Load & canonicalize** — normalizes `filter`, sorts `for_each_column`, stringifies `arguments`, builds canonical JSON payload, computes `check_id=sha256(payload)`.  
6) **Batch de-dupe by `check_id`** — keep the lexicographically first (by `yaml_path`, `name`); mode: `warn|error|skip`.  
7) **Diagnostics** — displays totals, sample rules, and counts by table.  
8) **Overwrite target table** — writes Delta with `overwriteSchema=true` and timestamp casting.  
9) **Apply documentation** — table comment (fallback to TBLPROPERTIES) + column comments (with ALTER COLUMN; fallback to COMMENT ON COLUMN).  
10) **PK & enforcement** — add/refresh **informational PK** on `check_id` (if UC) and **assert uniqueness** at runtime.  
11) **Result summary** — small confirmation table and a printed success line.

---

## Parameters (function `main`)

- `output_config_path`: YAML path for loader config (default `resources/dqx_config.yaml`).  
- `rules_dir`: override rules folder (else from config).  
- `time_zone`: used for audit timestamps.  
- `dry_run`: if `True`, loads + displays but **does not write**.  
- `validate_only`: if `True`, runs validations and **exits**.  
- `required_fields`: override minimal YAML fields (default set used).  
- `batch_dedupe_mode`: `warn` (print & keep first) | `error` (fail) | `skip` (no dedupe).  
- `table_doc`: optional dict to override default table/column comments.

---

## Prereqs & Permissions

- Python deps: `pyspark`, `pyyaml`, `databricks-labs-dqx (0.8.x)` available on the cluster/warehouse.  
- Data access: **READ** on `rules_dir` (workspace file system or `dbfs:/`), **CREATE/ALTER** on target catalog & schema, and **CREATE TABLE / ALTER TABLE** privileges to set comments/constraints.  
- Unity Catalog strongly recommended (for PK declaration). The run still proceeds if constraint DDL is not supported; a message is logged.

---

## Failure Modes & Fixes

- **Validation failed** (missing required fields, invalid `criticality`, `table_name` not fully qualified, DQEngine errors): fix YAML and re-run.  
- **Duplicate `check_id`** during de-dupe or final uniqueness assertion: adjust YAML so canonical payloads differ (e.g., rule name is *not* part of identity).  
- **Insufficient privileges** during write/comments/constraint: grant `CREATE/ALTER` on target schema/table, or run with a service principal that has the rights.  
- **Column comments not applied**: only columns present in the table are touched; fallback DDL is attempted and any skips are logged.

---

## Post‑Run Sanity Checks (SQL)

```sql
-- Table present & counts
SELECT COUNT(*) AS rules, COUNT(DISTINCT check_id) AS unique_rules FROM dq_dev.dqx.checks;

-- Any dup check_id? (should be 0 rows)
SELECT check_id, COUNT(*) c FROM dq_dev.dqx.checks GROUP BY check_id HAVING COUNT(*)>1;

-- Peek arguments & coverage
SELECT name, check.function, check.arguments FROM dq_dev.dqx.checks LIMIT 20;
```

---

## Notes

- Overwrite semantics: this is the **system of record** for rules derived from YAML; manual edits in the table will be lost on next run.  
- `check_id` identity includes only `{table_name↓, filter, check.*}` — **not** `name`, `criticality`, or `run_config_name`.  
- Argument values are persisted as strings for stability; cast/parse (e.g., `try_cast`, `from_json`) downstream.