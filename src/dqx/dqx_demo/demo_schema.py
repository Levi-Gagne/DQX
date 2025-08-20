from pyspark.sql.types import *

# 1) demo_employee
demo_employee_schema = StructType([
    StructField("employee_id", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("department", StringType(), False),        # Consulting, Audit, Tax, IT, Ops
    StructField("role", StringType(), False),               # Engineer, Analyst, Consultant, Manager, Support
    StructField("cost_center", StringType(), True),         # CC-####
    StructField("employment_status", StringType(), False),  # Active, Leave, Terminated
    StructField("hire_date", DateType(), False),
    StructField("termination_date", DateType(), True),
    StructField("work_email", StringType(), True),
    StructField("country_code", StringType(), True)
    # DQ columns appended later: dq_status, dq_error_codes, dq_warning_codes
])

# 2) demo_customer
demo_customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("industry", StringType(), False),          # Technology, Healthcare, Finance, Retail, Manufacturing
    StructField("country_code", StringType(), True),       # ISO-3166 alpha-2
    StructField("status", StringType(), False),            # Active, Prospect, Inactive
    StructField("onboarding_date", DateType(), False),
    StructField("primary_contact_email", StringType(), True),
    StructField("registration_number", StringType(), True) # national registration / tax-like identifier
])

# 3) demo_project
demo_project_schema = StructType([
    StructField("project_id", StringType(), False),
    StructField("customer_id", StringType(), False),       # FK -> demo_customer
    StructField("project_name", StringType(), False),
    StructField("status", StringType(), False),            # Planned, Active, OnHold, Closed
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("manager_employee_id", StringType(), True),# FK -> demo_employee
    StructField("budget_amount", DecimalType(18,2), True),
    StructField("billing_model", StringType(), False)      # T&M, Fixed, Retainer
])

# 4) demo_timesheet
demo_timesheet_schema = StructType([
    StructField("timesheet_id", StringType(), False),
    StructField("employee_id", StringType(), False),       # FK -> demo_employee
    StructField("project_id", StringType(), False),        # FK -> demo_project
    StructField("work_date", DateType(), False),
    StructField("hours_worked", DecimalType(5,2), False),  # 0–24
    StructField("work_type", StringType(), False),         # Billable, NonBillable, Admin
    StructField("source_system", StringType(), False),     # Workday, Jira, CSV
    StructField("created_ts", TimestampType(), False)
])

# 5) demo_expense
demo_expense_schema = StructType([
    StructField("expense_id", StringType(), False),
    StructField("employee_id", StringType(), False),       # FK -> demo_employee
    StructField("project_id", StringType(), True),         # FK -> demo_project (nullable)
    StructField("expense_date", DateType(), False),
    StructField("category", StringType(), False),          # Meals, Travel, Supplies, Software, Other
    StructField("amount", DecimalType(18,2), False),
    StructField("currency_code", StringType(), False),     # ISO-4217
    StructField("merchant", StringType(), True),
    StructField("receipt_attached", BooleanType(), False),
    StructField("submission_ts", TimestampType(), False)
])

# (Optional) quarantine sink for ERROR rows captured by DQX
demo_quarantine_schema = StructType([
    StructField("_source_table", StringType(), False),
    StructField("_rule_id", StringType(), False),
    StructField("_level", StringType(), False),            # ERROR / WARN
    StructField("_reason", StringType(), True),
    StructField("_run_id", StringType(), True),
    StructField("_event_ts", TimestampType(), False),
    StructField("_row_payload_json", StringType(), False)  # full offending row as JSON
])