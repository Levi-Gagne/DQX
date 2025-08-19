I guess in our main funciton can we have this logic: 

lets call this function first from:
utils.table.py:
def table_exists(spark: SparkSession, fully_qualified_table: str) -> bool:
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"{Print.ERROR}Exception in table_exists({fully_qualified_table}): {e}")
        return False


THen

if exists & Not apply_table_metadata
    contine
else create table setting everying, comments, table description, column comments, nullable etc


Can we do that? 

where 
apply_table_metadata

is added to:
res = load_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    # dry_run=True,
    # validate_only=True,
    batch_dedupe_mode="warn",
)
print(res)


SO it would be:
res = load_checks(
    dqx_cfg_yaml="resources/dqx_config.yaml",
    created_by="AdminUser",
    apply_table_metadata=False  
    # dry_run=True,
    # validate_only=True,
    batch_dedupe_mode="warn",
)
print(res)
