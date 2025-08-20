# =====================================================================================
# CELL 1 — demo_spec.py  (Schemas + MEGASPEC + rule-class stubs + table directory)
# =====================================================================================
# Purpose: single source of truth for table names, schemas, row targets, and knobs.
# =====================================================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DateType, TimestampType, DecimalType
)

# ──────────────────────────────────────────────────────────────────────────────
# Globals / conventions (Unity Catalog: CATALOG.SCHEMA.TABLE)
# ──────────────────────────────────────────────────────────────────────────────
DQX_CATALOG        = "dq_dev"
DQX_SCHEMA         = "dqx"
DQX_DB             = f"{DQX_CATALOG}.{DQX_SCHEMA}"           # catalog.schema
QUARANTINE_TABLE   = f"{DQX_DB}.demo_quarantine"             # sink for ERROR rows
DQ_OUTPUT_COLUMNS  = ["warning", "error"]                    # columns your runner appends

ROW_TARGETS = {
    f"{DQX_DB}.demo_employee":   2_000,
    f"{DQX_DB}.demo_customer":   1_000,
    f"{DQX_DB}.demo_project":      600,
    f"{DQX_DB}.demo_timesheet": 350_000,
    f"{DQX_DB}.demo_expense":   120_000,
}

# Canonical table names (full paths) + groupings you can use anywhere
TABLES = {
    "catalog":    DQX_CATALOG,
    "schema":     DQX_DB,   # fully-qualified (catalog.schema)
    "employee":   f"{DQX_DB}.demo_employee",
    "customer":   f"{DQX_DB}.demo_customer",
    "project":    f"{DQX_DB}.demo_project",
    "timesheet":  f"{DQX_DB}.demo_timesheet",
    "expense":    f"{DQX_DB}.demo_expense",
    "quarantine": QUARANTINE_TABLE,
    # Groupings
    "sources": [
        f"{DQX_DB}.demo_employee",
        f"{DQX_DB}.demo_customer",
        f"{DQX_DB}.demo_project",
        f"{DQX_DB}.demo_timesheet",
        f"{DQX_DB}.demo_expense",
    ],
    "facts": [
        f"{DQX_DB}.demo_timesheet",
        f"{DQX_DB}.demo_expense",
    ],
    "dims": [
        f"{DQX_DB}.demo_employee",
        f"{DQX_DB}.demo_customer",
        f"{DQX_DB}.demo_project",
    ],
    "all": [
        f"{DQX_DB}.demo_employee",
        f"{DQX_DB}.demo_customer",
        f"{DQX_DB}.demo_project",
        f"{DQX_DB}.demo_timesheet",
        f"{DQX_DB}.demo_expense",
        QUARANTINE_TABLE,
    ],
}

# ──────────────────────────────────────────────────────────────────────────────
# Spark Structured Schemas
# ──────────────────────────────────────────────────────────────────────────────

demo_employee_schema = StructType([
    StructField("employee_id",        StringType(),  False),
    StructField("full_name",          StringType(),  False),
    StructField("department",         StringType(),  False),
    StructField("role",               StringType(),  False),
    StructField("cost_center",        StringType(),  True),
    StructField("employment_status",  StringType(),  False),
    StructField("hire_date",          DateType(),    False),
    StructField("termination_date",   DateType(),    True),
    StructField("work_email",         StringType(),  True),
    StructField("country_code",       StringType(),  True),
    # DQ columns appended later: warning, error
])

demo_customer_schema = StructType([
    StructField("customer_id",            StringType(),  False),
    StructField("customer_name",          StringType(),  False),
    StructField("industry",               StringType(),  False),
    StructField("country_code",           StringType(),  True),
    StructField("status",                 StringType(),  False),
    StructField("onboarding_date",        DateType(),    False),
    StructField("primary_contact_email",  StringType(),  True),
    StructField("registration_number",    StringType(),  True),
])

demo_project_schema = StructType([
    StructField("project_id",          StringType(),       False),
    StructField("customer_id",         StringType(),       False),  # FK -> dq_dev.dqx.demo_customer
    StructField("project_name",        StringType(),       False),
    StructField("status",              StringType(),       False),
    StructField("start_date",          DateType(),         False),
    StructField("end_date",            DateType(),         True),
    StructField("manager_employee_id", StringType(),       True),   # FK -> dq_dev.dqx.demo_employee
    StructField("budget_amount",       DecimalType(18, 2), True),
    StructField("billing_model",       StringType(),       False),
])

demo_timesheet_schema = StructType([
    StructField("timesheet_id",  StringType(),       False),
    StructField("employee_id",   StringType(),       False),  # FK -> dq_dev.dqx.demo_employee
    StructField("project_id",    StringType(),       False),  # FK -> dq_dev.dqx.demo_project
    StructField("work_date",     DateType(),         False),
    StructField("hours_worked",  DecimalType(5, 2),  False),
    StructField("work_type",     StringType(),       False),
    StructField("source_system", StringType(),       False),
    StructField("created_ts",    TimestampType(),    False),
])

demo_expense_schema = StructType([
    StructField("expense_id",       StringType(),       False),
    StructField("employee_id",      StringType(),       False),  # FK -> dq_dev.dqx.demo_employee
    StructField("project_id",       StringType(),       True),   # FK -> dq_dev.dqx.demo_project (nullable)
    StructField("expense_date",     DateType(),         False),
    StructField("category",         StringType(),       False),
    StructField("amount",           DecimalType(18, 2), False),
    StructField("currency_code",    StringType(),       False),
    StructField("merchant",         StringType(),       True),
    StructField("receipt_attached", BooleanType(),      False),
    StructField("submission_ts",    TimestampType(),    False),
])

# Handy schema map (full paths)
SCHEMAS_BY_TABLE = {
    TABLES["employee"]:  demo_employee_schema,
    TABLES["customer"]:  demo_customer_schema,
    TABLES["project"]:   demo_project_schema,
    TABLES["timesheet"]: demo_timesheet_schema,
    TABLES["expense"]:   demo_expense_schema,
}

TABLE_ORDER = [
    TABLES["employee"],
    TABLES["customer"],
    TABLES["project"],
    TABLES["timesheet"],
    TABLES["expense"],
]

# ──────────────────────────────────────────────────────────────────────────────
# MEGASPEC (nested, readable). Map this to dbldatagen/ddgen in your builder.
# Fully qualified refs use the dq_dev.dqx schema.
# ──────────────────────────────────────────────────────────────────────────────

MEGASPEC = {
    "schema": DQX_DB,
    "quarantine_table": QUARANTINE_TABLE,
    "dq_output_columns": DQ_OUTPUT_COLUMNS,
    "flags": {
        "enforce_project_window": True,
        "allow_weekend_billable": "warn",   # 'warn' | 'error' | 'off'
    },
    "knobs": {
        "receipt_threshold": 75.00,
        "meal_limit": 150.00,
        "travel_limit": 500.00,
        "max_hours_per_day_error": 24.0,
        "hi_hours_warn": 12.0,
        "duplicate_timesheet_window_days": 0,
        "duplicate_expense_exact_match": True,
        "valid_currency_codes": ["USD","CAD","MXN","GBP","INR"],
    },
    "tables": {
        f"{DQX_DB}.demo_employee": {
            "rows": ROW_TARGETS[f"{DQX_DB}.demo_employee"],
            "cols": {
                "employee_id": {"type":"sequence","prefix":"E","start":1001},
                "full_name": {"type":"faker","provider":"name"},
                "department": {"type":"choice","values":["Consulting","Audit","Tax","IT","Ops"],"weights":[0.35,0.20,0.20,0.15,0.10]},
                "role": {"type":"choice","values":["Engineer","Analyst","Consultant","Manager","Support"]},
                "cost_center": {"type":"pattern","format":"CC-{0000-9999}","null_rate":0.02},
                "employment_status": {"type":"choice","values":["Active","Leave","Terminated"],"weights":[0.90,0.03,0.07]},
                "hire_date": {"type":"date","start":"2018-01-01","end":"2025-08-01"},
                "termination_date": {"type":"conditional_date","when":"employment_status == 'Terminated'","offset_from":"hire_date","min_days":30,"max_days":2500,"null_rate":0.08},
                "work_email": {"type":"template","template":"{first}.{last}@company.com","invalid_rate":0.03},
                "country_code": {"type":"choice","values":["US","CA","MX","GB","IN"],"invalid_rate":0.005}
            },
            "inject": {"EMP_TERM_DATE_BEFORE_HIRE": 0.008}
        },

        f"{DQX_DB}.demo_customer": {
            "rows": ROW_TARGETS[f"{DQX_DB}.demo_customer"],
            "cols": {
                "customer_id": {"type":"sequence","prefix":"C","start":5001},
                "customer_name": {"type":"faker","provider":"company"},
                "industry": {"type":"choice","values":["Technology","Healthcare","Finance","Retail","Manufacturing"],"weights":[0.30,0.20,0.20,0.20,0.10]},
                "country_code": {"type":"choice","values":["US","CA","MX","GB","IN"],"invalid_rate":0.005},
                "status": {"type":"choice","values":["Active","Prospect","Inactive"],"weights":[0.75,0.15,0.10]},
                "onboarding_date": {"type":"date","start":"2019-01-01","end":"2025-08-01"},
                "primary_contact_email": {"type":"faker","provider":"email","invalid_rate":0.02},
                "registration_number": {"type":"pattern","format":"RN-{00000000-99999999}","dupe_rate_active":0.01}
            }
        },

        f"{DQX_DB}.demo_project": {
            "rows": ROW_TARGETS[f"{DQX_DB}.demo_project"],
            "cols": {
                "project_id": {"type":"sequence","prefix":"P","start":10001},
                "customer_id": {"type":"fk","ref": f"{DQX_DB}.demo_customer.customer_id","null_rate":0.01},
                "project_name": {"type":"template","template":"Project {seq}"},
                "status": {"type":"choice","values":["Planned","Active","OnHold","Closed"],"weights":[0.15,0.55,0.10,0.20]},
                "start_date": {"type":"date","start":"2020-01-01","end":"2025-03-01"},
                "end_date": {"type":"date_or_null","null_rate":0.35,"min_from":"start_date","min_days":30,"max_days":1200},
                "manager_employee_id": {"type":"fk","ref": f"{DQX_DB}.demo_employee.employee_id","null_rate":0.03},
                "budget_amount": {"type":"decimal","min":10_000.00,"max":2_000_000.00,"skew":"right"},
                "billing_model": {"type":"choice","values":["T&M","Fixed","Retainer"],"weights":[0.55,0.35,0.10]}
            },
            "inject": {"PROJ_END_BEFORE_START":0.007,"PROJ_BUDGET_NONPOSITIVE":0.004}
        },

        f"{DQX_DB}.demo_timesheet": {
            "rows": ROW_TARGETS[f"{DQX_DB}.demo_timesheet"],
            "cols": {
                "timesheet_id": {"type":"uuid"},
                "employee_id": {"type":"fk","ref": f"{DQX_DB}.demo_employee.employee_id"},
                "project_id": {"type":"fk","ref": f"{DQX_DB}.demo_project.project_id"},
                "work_date": {"type":"date","start":"2024-01-01","end":"2025-08-10"},
                "hours_worked": {"type":"decimal","min":0.00,"max":12.00,"step":0.25},
                "work_type": {"type":"choice","values":["Billable","NonBillable","Admin"],"weights":[0.70,0.25,0.05]},
                "source_system": {"type":"choice","values":["Workday","Jira","CSV"],"weights":[0.70,0.20,0.10]},
                "created_ts": {"type":"timestamp_near","base":"work_date","min_offset_hours":0,"max_offset_hours":72}
            },
            "inject": {"TS_FUTURE_DATE":0.003,"TS_DUP_EMP_PROJ_DAY":0.006,"TS_NEG_OR_GT24":0.003}
        },

        f"{DQX_DB}.demo_expense": {
            "rows": ROW_TARGETS[f"{DQX_DB}.demo_expense"],
            "cols": {
                "expense_id": {"type":"uuid"},
                "employee_id": {"type":"fk","ref": f"{DQX_DB}.demo_employee.employee_id"},
                "project_id": {"type":"fk","ref": f"{DQX_DB}.demo_project.project_id","null_rate":0.25},
                "expense_date": {"type":"date","start":"2024-01-01","end":"2025-08-10"},
                "category": {"type":"choice","values":["Meals","Travel","Supplies","Software","Other"],"weights":[0.35,0.25,0.20,0.10,0.10]},
                "amount": {"type":"decimal","min":5.00,"max":5_000.00},
                "currency_code": {"type":"choice","values":["USD","CAD","MXN","GBP","INR"],"invalid_rate":0.002},
                "merchant": {"type":"choice","values":["Uber","Lyft","Delta","AA","Staples","BestBuy","Amazon","LocalCafe","HotelCo","SoftwareCo"]},
                "receipt_attached": {"type":"boolean","true_rate":0.92},
                "submission_ts": {"type":"timestamp_near","base":"expense_date","min_offset_hours":0,"max_offset_hours":240}
            },
            "inject": {"EXP_DUP":0.008,"EXP_NO_RECEIPT_OVER_75":0.010,"EXP_OUT_OF_POLICY":0.020}
        }
    }
}

# ──────────────────────────────────────────────────────────────────────────────
# Rule classes (stubs). Define the .rules() bodies in a separate module or here.
# ──────────────────────────────────────────────────────────────────────────────

class BaseRuleSet:
    """Interface for a rule set; implement .rules() to return a list of rule dicts"""
    def rules(self):
        raise NotImplementedError("Implement .rules() to return a list of rule dicts")

class EmployeeRules(BaseRuleSet):
    """dq_dev.dqx.demo_employee — suggested IDs: EMP_TERM_DATE_MISSING, EMP_TERM_DATE_BEFORE_HIRE, EMP_EMAIL_DOMAIN_WARN, EMP_COUNTRY_CODE_WARN"""
    pass

class CustomerRules(BaseRuleSet):
    """dq_dev.dqx.demo_customer — suggested IDs: CUST_REG_DUP_ACTIVE, CUST_EMAIL_WARN, CUST_COUNTRY_CODE_WARN"""
    pass

class ProjectRules(BaseRuleSet):
    """dq_dev.dqx.demo_project — suggested IDs: PROJ_END_BEFORE_START, PROJ_BUDGET_NONPOSITIVE, PROJ_FK_CUSTOMER_MISSING, PROJ_MANAGER_FK_WARN, PROJ_CLOSED_END_NULL_WARN"""
    pass

class TimesheetRules(BaseRuleSet):
    """dq_dev.dqx.demo_timesheet — suggested IDs: TS_NEG_OR_GT24, TS_HI_HOURS_WARN, TS_FUTURE_DATE, TS_EMP_STATUS_ERROR, TS_DUP_EMP_PROJ_DAY, TS_OUTSIDE_PROJECT_WINDOW, TS_WEEKEND_BILLABLE"""
    pass

class ExpenseRules(BaseRuleSet):
    """dq_dev.dqx.demo_expense — suggested IDs: EXP_DUP, EXP_NO_RECEIPT_OVER_T, EXP_OOP, EXP_BAD_CCY, EXP_EMP_STATUS_ERROR, EXP_OUTSIDE_PROJECT_WINDOW"""
    pass

# Map table → rule class so a driver can instantiate dynamically
RULE_CLASSES_BY_TABLE = {
    TABLES["employee"]:  EmployeeRules,
    TABLES["customer"]:  CustomerRules,
    TABLES["project"]:   ProjectRules,
    TABLES["timesheet"]: TimesheetRules,
    TABLES["expense"]:   ExpenseRules,
}

print(f"\n[SPEC LOADED] schema={DQX_DB}  sources={len(TABLES['sources'])}  quarantine={TABLES['quarantine']}")
print("Row targets: " + ", ".join([f"{k.split('.')[-1]}={v:,}" for k,v in ROW_TARGETS.items()]))

# COMMAND ----------
# =====================================================================================
# CELL 2 — Driver setup (imports, UC catalog/schema, quarantine table)
# =====================================================================================

from pyspark.sql import functions as F
from pyspark.sql import Window
from uuid import uuid4

# Pull in the spec from Cell 1 (already in-memory if same notebook)
from __main__ import (
    DQX_CATALOG, DQX_SCHEMA, DQX_DB,
    TABLES, ROW_TARGETS, SCHEMAS_BY_TABLE, MEGASPEC,
    RULE_CLASSES_BY_TABLE, QUARANTINE_TABLE
)

def banner(msg):
    print("\n" + "═"*88)
    print(f" {msg}")
    print("═"*88)

banner("SETUP: Use dq_dev catalog, ensure dqx schema, create quarantine table")

# Unity Catalog-aware setup
spark.sql(f"USE CATALOG {DQX_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DQX_DB}")
spark.sql(f"USE {DQX_DB}")

run_id = str(uuid4())
print(f"Using: catalog={DQX_CATALOG}  schema={DQX_SCHEMA}  (db={DQX_DB})")
print(f"Run ID: {run_id}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
  _source_table     STRING,
  _rule_id          STRING,
  _level            STRING,
  _reason           STRING,
  _run_id           STRING,
  _event_ts         TIMESTAMP,
  _row_payload_json STRING
) USING delta
""")
print(f"Quarantine table ready: {QUARANTINE_TABLE}")

# COMMAND ----------
# =====================================================================================
# CELL 3 — Helpers (small utilities for deterministic, lightweight data gen)
# =====================================================================================

banner("HELPERS: small UDF-like helpers (still available; Datagen used below)")

def seq_id(prefix: str, start: int):
    return F.concat(F.lit(prefix), F.lpad((F.col("_rn") + F.lit(start)).cast("string"), 4, "0"))

def choice_expr(options, weights=None, seed=None):
    # Kept for reference; not used in Datagen flow
    if not options or len(options) == 0:
        raise ValueError("options must be non-empty")
    if not weights:
        weights = [1.0/len(options)] * len(options)
    elif len(weights) != len(options):
        raise ValueError("weights length must match options length")
    total = float(sum(weights))
    cum = []
    s = 0.0
    for w in weights:
        s += float(w) / total
        cum.append(s)
    rnd = F.rand(seed)
    expr = F.lit(options[-1])
    for i in range(len(options) - 2, -1, -1):
        expr = F.when(rnd <= F.lit(cum[i]), F.lit(options[i])).otherwise(expr)
    return expr

def rand_date(start: str, end: str, rand_expr="rand()"):
    days = F.datediff(F.lit(end), F.lit(start))
    return F.expr(f"date_add('{start}', cast({rand_expr} * {days} as int))")

print("Helpers ready (Datagen used in Cells 4–8).")

# COMMAND ----------
# =====================================================================================
# CELL 4 — DIM: Employees (dbldatagen)
# =====================================================================================

banner(f"BUILD (Datagen): {TABLES['employee']}")

try:
    from dbldatagen import DataGenerator
except Exception as e:
    print("ERROR: dbldatagen not available. Install it with `%pip install dbldatagen` in a separate cell and re-run.")
    raise

n_emp = ROW_TARGETS[TABLES["employee"]]

emp_gen = (DataGenerator(spark, name="demo_employee_gen", rows=n_emp, partitions=8)
           .withIdOutput()
           .withColumn("employee_id", "string",
                       expr="concat('E', lpad(cast(id + 1001 as string), 4, '0'))")
           .withColumn("full_name", "string",
                       expr="concat('Emp ', cast(id as string))")
           .withColumn("department", values=["Consulting","Audit","Tax","IT","Ops"],
                       weights=[0.35,0.20,0.20,0.15,0.10])
           .withColumn("role", values=["Engineer","Analyst","Consultant","Manager","Support"])
           .withColumn("cost_center", "string",
                       expr="concat('CC-', lpad(cast(cast(rand()*10000 as int) as string), 4, '0'))",
                       percentNulls=0.02)
           .withColumn("employment_status", values=["Active","Leave","Terminated"],
                       weights=[0.90,0.03,0.07])
           .withColumn("hire_date", "date", minValue="2018-01-01", maxValue="2025-08-01")
           .withColumn("termination_date", "date", minValue="2018-02-01", maxValue="2030-12-31",
                       percentNulls=1.0)  # will compute conditionally below
           .withColumn("work_email", "string",
                       expr="concat('emp', cast(id as string), '@company.com')")
           .withColumn("country_code", values=["US","CA","MX","GB","IN"])
)

emp_df = (emp_gen.build()
          .drop("id"))

# Conditional injections to mirror MEGASPEC
emp_df = (emp_df
    .withColumn(
        "termination_date",
        F.when(
            (F.col("employment_status")=="Terminated") & (F.rand(7) < 0.08),
            F.lit(None).cast("date")
        ).otherwise(
            F.when(F.col("employment_status")=="Terminated",
                   F.expr("date_add(hire_date, cast(rand(8)*2500 + 30 as int))"))
             .otherwise(F.lit(None).cast("date"))
        )
    )
    .withColumn(
        "work_email",
        F.when(F.rand(9) < 0.03,
               F.regexp_replace(F.col("work_email"), "@company\\.com$", "@gmail.com"))
         .otherwise(F.col("work_email"))
    )
    .withColumn(
        "country_code",
        F.when(F.rand(10) < 0.005, F.lit("XX")).otherwise(F.col("country_code"))
    )
)

emp_df.write.format("delta").mode("overwrite").saveAsTable(TABLES["employee"])
print(f"Wrote: {TABLES['employee']}  rows={spark.table(TABLES['employee']).count():,}")
display(spark.table(TABLES["employee"]).limit(5))

# COMMAND ----------
# =====================================================================================
# CELL 5 — DIM: Customers (dbldatagen)
# =====================================================================================

banner(f"BUILD (Datagen): {TABLES['customer']}")

from dbldatagen import DataGenerator

n_cust = ROW_TARGETS[TABLES["customer"]]

cust_gen = (DataGenerator(spark, name="demo_customer_gen", rows=n_cust, partitions=8)
            .withIdOutput()
            .withColumn("customer_id", "string",
                        expr="concat('C', lpad(cast(id + 5001 as string), 4, '0'))")
            .withColumn("customer_name", "string",
                        expr="concat('Customer ', cast(id as string))")
            .withColumn("industry", values=["Technology","Healthcare","Finance","Retail","Manufacturing"],
                        weights=[0.30,0.20,0.20,0.20,0.10])
            .withColumn("country_code", values=["US","CA","MX","GB","IN"])
            .withColumn("status", values=["Active","Prospect","Inactive"],
                        weights=[0.75,0.15,0.10])
            .withColumn("onboarding_date", "date", minValue="2019-01-01", maxValue="2025-08-01")
            .withColumn("primary_contact_email", "string",
                        expr="concat('contact', cast(id as string), '@example.com')")
            .withColumn("registration_number", "string",
                        expr="concat('RN-', lpad(cast(cast(rand()*100000000 as int) as string), 8, '0'))")
)

cust_df = cust_gen.build().drop("id")

# Duplicate registration_number among Actives (~1%)
dupe_sample = (cust_df.where(F.col("status")=="Active")
               .select("registration_number")
               .limit(int(n_cust * 0.01))
               .withColumnRenamed("registration_number","dup_reg"))

cust_df = (cust_df.crossJoin(dupe_sample.limit(1))
           .withColumn(
               "registration_number",
               F.when(F.rand(123) < 0.01, F.col("dup_reg")).otherwise(F.col("registration_number"))
           ).drop("dup_reg"))

cust_df.write.format("delta").mode("overwrite").saveAsTable(TABLES["customer"])
print(f"Wrote: {TABLES['customer']}  rows={spark.table(TABLES['customer']).count():,}")
display(spark.table(TABLES["customer"]).limit(5))

# COMMAND ----------
# =====================================================================================
# CELL 6 — DIM: Projects (dbldatagen, FKs to Customer & Employee)
# =====================================================================================

banner(f"BUILD (Datagen): {TABLES['project']}")

from dbldatagen import DataGenerator

n_proj = ROW_TARGETS[TABLES["project"]]

emp_ids = [r[0] for r in spark.table(TABLES["employee"]).select("employee_id").collect()]
cust_ids = [r[0] for r in spark.table(TABLES["customer"]).select("customer_id").collect()]

proj_gen = (DataGenerator(spark, name="demo_project_gen", rows=n_proj, partitions=8)
            .withIdOutput()
            .withColumn("project_id", "string",
                        expr="concat('P', lpad(cast(id + 10001 as string), 5, '0'))")
            .withColumn("customer_id", values=cust_ids)
            .withColumn("project_name", "string",
                        expr="concat('Project ', cast(id as string))")
            .withColumn("status", values=["Planned","Active","OnHold","Closed"],
                        weights=[0.15,0.55,0.10,0.20])
            .withColumn("start_date", "date", minValue="2020-01-01", maxValue="2025-03-01")
            .withColumn("end_date", "date", minValue="2020-02-01", maxValue="2028-12-31",
                        percentNulls=0.35)
            .withColumn("manager_employee_id", values=emp_ids, percentNulls=0.03)
            .withColumn("budget_amount", "decimal(18,2)", minValue=10000, maxValue=2000000)
            .withColumn("billing_model", values=["T&M","Fixed","Retainer"],
                        weights=[0.55,0.35,0.10])
)

proj_df = proj_gen.build().drop("id")

# Inject errors: end before start (~0.7%), non-positive budget (~0.4%)
proj_df = (proj_df
    .withColumn("end_date",
        F.when(F.rand(7) < 0.007,
               F.expr("date_add(start_date, -cast(rand()*90 + 1 as int))"))
         .otherwise(F.col("end_date")))
    .withColumn("budget_amount",
        F.when(F.rand(8) < 0.004, F.lit(0).cast("decimal(18,2)"))
         .otherwise(F.col("budget_amount")))
)

proj_df.write.format("delta").mode("overwrite").saveAsTable(TABLES["project"])
print(f"Wrote: {TABLES['project']}  rows={spark.table(TABLES['project']).count():,}")
display(spark.table(TABLES["project"]).limit(5))

# COMMAND ----------
# =====================================================================================
# CELL 7 — FACT: Timesheets (dbldatagen, FKs to Employee & Project)
# =====================================================================================

banner(f"BUILD (Datagen): {TABLES['timesheet']}")

from dbldatagen import DataGenerator

n_ts = ROW_TARGETS[TABLES["timesheet"]]
emp_ids = [r[0] for r in spark.table(TABLES["employee"]).select("employee_id").collect()]
proj_keys = (spark.table(TABLES["project"])
             .select("project_id","start_date","end_date")
             .collect())

project_id_list = [r["project_id"] for r in proj_keys]
proj_dates_map = {r["project_id"]:(r["start_date"], r["end_date"]) for r in proj_keys}

ts_gen = (DataGenerator(spark, name="demo_timesheet_gen", rows=n_ts, partitions=48)
          .withColumn("timesheet_id", "string", expr="uuid()")
          .withColumn("employee_id", values=emp_ids)
          .withColumn("project_id", values=project_id_list)
          .withColumn("work_date", "date", minValue="2024-01-01", maxValue="2025-08-10")
          .withColumn("hours_worked", "decimal(5,2)", minValue=0.00, maxValue=12.00)
          .withColumn("work_type", values=["Billable","NonBillable","Admin"],
                      weights=[0.70,0.25,0.05])
          .withColumn("source_system", values=["Workday","Jira","CSV"],
                      weights=[0.70,0.20,0.10])
          .withColumn("created_ts", "timestamp")
)

ts_df = ts_gen.build()

# created_ts near work_date (0–72 hours)
ts_df = ts_df.withColumn(
    "created_ts",
    F.expr("timestamp(work_date) + make_interval(0,0,0,cast(rand()*3 as int), cast(rand()*24 as int), cast(rand()*60 as int), 0)")
)

# Injections: future dates (~0.3%), impossible hours (~0.3%), duplicates (~0.6%)
ts_df = (ts_df
    .withColumn("work_date",
        F.when(F.rand(3) < 0.003,
               F.expr("date_add(current_date(), cast(rand()*5 + 1 as int))"))
         .otherwise(F.col("work_date")))
    .withColumn("hours_worked",
        F.when(F.rand(4) < 0.003,
               F.when(F.rand(5) < 0.5, F.lit(-1.0)).otherwise(F.lit(25.0)))
         .otherwise(F.col("hours_worked")))
)

dupe_fraction = 0.006
ts_df = ts_df.unionByName(ts_df.sample(withReplacement=True, fraction=dupe_fraction, seed=11))

ts_df.write.format("delta").mode("overwrite").saveAsTable(TABLES["timesheet"])
print(f"Wrote: {TABLES['timesheet']}  rows={spark.table(TABLES['timesheet']).count():,}")
display(spark.table(TABLES["timesheet"]).limit(5))

# COMMAND ----------
# =====================================================================================
# CELL 8 — FACT: Expenses (dbldatagen, FKs to Employee & Project)
# =====================================================================================

banner(f"BUILD (Datagen): {TABLES['expense']}")

from dbldatagen import DataGenerator

n_exp = ROW_TARGETS[TABLES["expense"]]
emp_ids = [r[0] for r in spark.table(TABLES["employee"]).select("employee_id").collect()]
proj_ids = [r[0] for r in spark.table(TABLES["project"]).select("project_id").collect()]

exp_gen = (DataGenerator(spark, name="demo_expense_gen", rows=n_exp, partitions=24)
           .withColumn("expense_id", "string", expr="uuid()")
           .withColumn("employee_id", values=emp_ids)
           .withColumn("project_id", values=proj_ids, percentNulls=0.25)
           .withColumn("expense_date", "date", minValue="2024-01-01", maxValue="2025-08-10")
           .withColumn("category", values=["Meals","Travel","Supplies","Software","Other"],
                       weights=[0.35,0.25,0.20,0.10,0.10])
           .withColumn("amount", "decimal(18,2)", minValue=5.00, maxValue=5000.00)
           .withColumn("currency_code", values=["USD","CAD","MXN","GBP","INR"])
           .withColumn("merchant", values=["Uber","Lyft","Delta","AA","Staples","BestBuy","Amazon","LocalCafe","HotelCo","SoftwareCo"])
           .withColumn("receipt_attached", "boolean", expr="rand() > 0.08")
           .withColumn("submission_ts", "timestamp")
)

exp_df = exp_gen.build()

# submission_ts near expense_date (0–240 hours)
exp_df = exp_df.withColumn(
    "submission_ts",
    F.expr("timestamp(expense_date) + make_interval(0,0,0,cast(rand()*10 as int), cast(rand()*24 as int), cast(rand()*60 as int), 0)")
)

# injections: invalid currency (~0.2%), receipt missing over threshold (~1%), duplicates (~0.8%)
exp_df = (exp_df
    .withColumn("currency_code",
        F.when(F.rand(2) < 0.002, F.lit("XXX")).otherwise(F.col("currency_code")))
    .withColumn("receipt_attached",
        F.when((F.col("amount") >= F.lit(MEGASPEC["knobs"]["receipt_threshold"])) & (F.rand(3) < 0.5), F.lit(False))
         .otherwise(F.col("receipt_attached")))
)

# build dup set on (employee_id, merchant, expense_date, amount)
dup_frac = 0.008
dups = (exp_df.sample(withReplacement=True, fraction=dup_frac, seed=12)
        .select("employee_id","merchant","expense_date","amount")
        .withColumn("k", F.lit(1)))
exp_df = (exp_df.join(dups.drop("k"), ["employee_id","merchant","expense_date","amount"], "left")
               .unionByName(exp_df.where(F.col("k").isNotNull()).drop("k"))
               .withColumn("k", F.lit(None).cast("int")))

exp_df.write.format("delta").mode("overwrite").saveAsTable(TABLES["expense"])
print(f"Wrote: {TABLES['expense']}  rows={spark.table(TABLES['expense']).count():,}")
display(spark.table(TABLES["expense"]).limit(5))

# COMMAND ----------
# =====================================================================================
# CELL 9 — Rules: default builder (used if your RULE_CLASSES are still stubs)
# =====================================================================================

banner("RULES: load from classes or fall back to default demo rules")

def build_default_rules():
    k = MEGASPEC["knobs"]
    valid_ccy = ",".join([f"'{c}'" for c in k["valid_currency_codes"]])
    weekend_level = MEGASPEC["flags"]["allow_weekend_billable"]
    weekend_level = "warn" if weekend_level not in ("warn","error") else weekend_level

    return {
      TABLES["employee"]: [
        {"id":"EMP_TERM_DATE_MISSING","level":"error","message":"Terminated missing termination_date",
         "expr":"employment_status = 'Terminated' AND termination_date IS NULL"},
        {"id":"EMP_TERM_DATE_BEFORE_HIRE","level":"error","message":"termination_date < hire_date",
         "expr":"employment_status = 'Terminated' AND termination_date < hire_date"},
        {"id":"EMP_EMAIL_DOMAIN_WARN","level":"warn","message":"Non-company or missing email",
         "expr":"work_email IS NULL OR NOT work_email LIKE '%@company.com'"},
      ],
      TABLES["customer"]: [
        {"id":"CUST_REG_DUP_ACTIVE","level":"error","message":"Duplicate registration_number among Active",
         "expr": f"""
            registration_number IS NOT NULL AND status = 'Active' AND registration_number IN (
              SELECT registration_number FROM {TABLES["customer"]}
              WHERE registration_number IS NOT NULL AND status = 'Active'
              GROUP BY registration_number HAVING COUNT(*) > 1
            )
         """},
      ],
      TABLES["project"]: [
        {"id":"PROJ_END_BEFORE_START","level":"error","message":"end_date before start_date",
         "expr":"end_date IS NOT NULL AND end_date < start_date"},
        {"id":"PROJ_FK_CUSTOMER_MISSING","level":"error","message":"customer_id not found",
         "expr":f"customer_id NOT IN (SELECT customer_id FROM {TABLES['customer']})"},
      ],
      TABLES["timesheet"]: [
        {"id":"TS_NEG_OR_GT24","level":"error","message":"Hours must be within 0..24",
         "expr":f"hours_worked < 0 OR hours_worked > {k['max_hours_per_day_error']}"},
        {"id":"TS_FUTURE_DATE","level":"error","message":"Work date in future",
         "expr":"work_date > current_date()"},
        {"id":"TS_EMP_STATUS_ERROR","level":"error","message":"Non-active employee",
         "expr":f"employee_id IN (SELECT employee_id FROM {TABLES['employee']} WHERE employment_status <> 'Active')"},
        {"id":"TS_DUP_EMP_PROJ_DAY","level":"warn","message":"Duplicate emp+proj+day",
         "expr":f"""
           (employee_id, project_id, work_date) IN (
             SELECT employee_id, project_id, work_date
             FROM {TABLES['timesheet']}
             GROUP BY employee_id, project_id, work_date
             HAVING COUNT(*) > 1
           )
         """},
        {"id":"TS_OUTSIDE_PROJECT_WINDOW","level":"error","message":"Work outside project window",
         "expr":f"""
           work_date < (SELECT start_date FROM {TABLES['project']} p WHERE p.project_id = {TABLES['timesheet']}.project_id)
           OR (
              (SELECT end_date FROM {TABLES['project']} p2 WHERE p2.project_id = {TABLES['timesheet']}.project_id) IS NOT NULL
              AND work_date >
                  (SELECT end_date FROM {TABLES['project']} p3 WHERE p3.project_id = {TABLES['timesheet']}.project_id)
           )
         """},
        {"id":"TS_WEEKEND_BILLABLE","level":weekend_level,"message":"Billable hours on weekend",
         "expr":"work_type = 'Billable' AND date_format(work_date, 'E') IN ('Sat','Sun')"},
      ],
      TABLES["expense"]: [
        {"id":"EXP_DUP","level":"error","message":"Duplicate expense",
         "expr":f"""
           (employee_id, merchant, expense_date, amount) IN (
             SELECT employee_id, merchant, expense_date, amount
             FROM {TABLES['expense']}
             GROUP BY employee_id, merchant, expense_date, amount
             HAVING COUNT(*) > 1
           )
         """},
        {"id":"EXP_NO_RECEIPT_OVER_T","level":"error","message":"Receipt required at/over threshold",
         "expr":f"amount >= {k['receipt_threshold']} AND receipt_attached = false"},
        {"id":"EXP_OOP","level":"warn","message":"Out-of-policy amount (Meals>limit or Travel>limit)",
         "expr":f"(category = 'Meals' AND amount > {k['meal_limit']}) OR (category = 'Travel' AND amount > {k['travel_limit']})"},
        {"id":"EXP_BAD_CCY","level":"error","message":"Invalid currency code",
         "expr":f"currency_code NOT IN ({valid_ccy})"},
        {"id":"EXP_EMP_STATUS_ERROR","level":"error","message":"Non-active employee",
         "expr":f"employee_id IN (SELECT employee_id FROM {TABLES['employee']} WHERE employment_status <> 'Active')"},
        {"id":"EXP_OUTSIDE_PROJECT_WINDOW","level":"error","message":"Expense outside project window",
         "expr":f"""
           project_id IS NOT NULL AND (
             expense_date < (SELECT start_date FROM {TABLES['project']} p WHERE p.project_id = {TABLES['expense']}.project_id)
             OR (
               (SELECT end_date FROM {TABLES['project']} p2 WHERE p2.project_id = {TABLES['expense']}.project_id) IS NOT NULL
               AND expense_date >
                   (SELECT end_date FROM {TABLES['project']} p3 WHERE p3.project_id = {TABLES['expense']}.project_id)
             )
           )
         """},
      ]
    }

def load_rules():
    rules_by_table = {}
    for tbl, cls in RULE_CLASSES_BY_TABLE.items():
        try:
            rset = cls()
            rules = rset.rules()  # should return list[dict]
            rules_by_table[tbl] = rules or []
        except Exception:
            rules_by_table[tbl] = []
    defaults = build_default_rules()
    for tbl in defaults:
        if tbl not in rules_by_table or not rules_by_table[tbl]:
            rules_by_table[tbl] = defaults[tbl]
    return rules_by_table

RULES = load_rules()
print("Rules loaded for:")
for k in TABLES["sources"]:
    print("  •", k, f"({len(RULES.get(k, []))} rules)")

# COMMAND ----------
# =====================================================================================
# CELL 10 — Rule Runner (append warning/error, write quarantine, keep clean rows)
# =====================================================================================

banner("APPLY: Run rules per table, quarantine errors, keep warned rows")

def apply_rules_to_table(table_fullname: str, rules: list, run_id: str):
    if not rules:
        print(f"[{table_fullname}] No rules; skipping.")
        return

    errs  = [r for r in rules if r.get("level","").lower()=="error"]
    warns = [r for r in rules if r.get("level","").lower()=="warn"]

    def _make_array_expr(rule_list):
        if not rule_list:
            return "array()"
        parts = [f"IF({r['expr']}, '{r['id']}', NULL)" for r in rule_list]
        return f"array_remove(array({', '.join(parts)}), NULL)"

    error_arr_sql = _make_array_expr(errs)
    warn_arr_sql  = _make_array_expr(warns)

    flagged_sql = f"""
      SELECT
        t.*,
        {warn_arr_sql}  AS warning,
        {error_arr_sql} AS error
      FROM {table_fullname} t
    """
    flagged = spark.sql(flagged_sql)

    if errs:
        err_map = spark.createDataFrame(
            [(e["id"], e.get("message","")) for e in errs],
            "rule_id STRING, message STRING"
        )
        error_rows = flagged.where(F.size(F.col("error")) > 0)
        exploded = (error_rows.select(
            F.lit(table_fullname).alias("_source_table"),
            F.explode(F.col("error")).alias("_rule_id"),
            F.lit("ERROR").alias("_level"),
            F.lit(run_id).alias("_run_id"),
            F.current_timestamp().alias("_event_ts"),
            F.to_json(F.struct([F.col(c) for c in flagged.columns])).alias("_row_payload_json")
        ).join(err_map, F.col("_rule_id")==F.col("rule_id"), "left")
         .select("_source_table","_rule_id","_level",F.col("message").alias("_reason"),"_run_id","_event_ts","_row_payload_json"))
        exploded.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

    cleaned = flagged.where(F.size(F.col("error")) == 0)
    cleaned.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(table_fullname)

    total  = flagged.count()
    err_ct = flagged.where(F.size(F.col("error")) > 0).count()
    warn_ct= cleaned.where(F.size(F.col("warning")) > 0).count()
    print(f"[{table_fullname}] total={total:,}  quarantined={err_ct:,}  warned_on_kept={warn_ct:,}")

for tbl in TABLES["sources"]:
    apply_rules_to_table(tbl, RULES.get(tbl, []), run_id)

print("Done applying rules.")

# COMMAND ----------
# =====================================================================================
# CELL 11 — Demo Views (summaries you can show)
# =====================================================================================

banner("DEMO: Quarantine summary by rule")
display(spark.sql(f"""
SELECT _rule_id, COUNT(*) AS hits
FROM {QUARANTINE_TABLE}
WHERE _run_id = '{run_id}'
GROUP BY _rule_id
ORDER BY hits DESC
"""))

banner("DEMO: Which tables had warnings?")
for tbl in TABLES["sources"]:
    df = spark.table(tbl)
    if "warning" in df.columns:
        cnt = df.where(F.size(F.col("warning")) > 0).count()
        print(f"  {tbl}: {cnt:,} rows with warnings")

banner("DEMO: Sample warned rows (expenses)")
display(spark.sql(f"""
SELECT expense_id, employee_id, category, amount, currency_code, receipt_attached, warning
FROM {TABLES['expense']}
WHERE size(warning) > 0
LIMIT 20
"""))

banner("DEMO: Row counts after quarantine")
display(spark.sql(f"SHOW TABLES IN {DQX_DB}"))
for tbl in TABLES["sources"]:
    print(tbl, spark.table(tbl).count())