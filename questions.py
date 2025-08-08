So i have a few questions about some of these checks i created


So below are two differnt checks:
# 1) Row-level: basic presence
- table_name: dq_prd.monitoring.job_run_audit
  name: run_id_is_not_null
  criticality: error
  run_config_name: default
  check:
    function: is_not_null
    arguments:
      column: run_id

# 2) Dataset-level: unique key
- table_name: dq_prd.monitoring.job_run_audit
  name: run_id_is_unique
  criticality: error
  run_config_name: default
  check:
    function: is_unique
    arguments:
      columns: [run_id]


what im curisou about is one has [] around the columns and one does not; which is correct?
