So it runs, and it creates teh table, but are we actually applying the checks on the data?



I get this as an ouput:
RUN-CONFIG: default
none has no output location — skipping.
TOTAL rows written: 0


what does none has no ouput location mean? 


please calrify that as im not sure what that is supposed to mean

we need to make sure we are getting all the information to run the checks from the table then running those against the data


Can we say if any hit or not and which ones?

For clarification I dont want us to 


Also can you verify, as the dqx documentation suggests (https://databrickslabs.github.io/dqx/docs/guide/quality_checks/#applying-quality-rules-defined-in-a-delta-table)

that we are using one of these two methods?
apply_checks_by_metadata_and_split: splits the input data into valid and invalid (quarantined) dataframes.

or 

  apply_checks_by_metadata: report issues as additional columns.


not i dont want the results saved to the table as an addiitonal table but teh resuttls should go into the table we defined here:
dqx_checks_log_table_name: dq_dev.dqx.checks_log


Its the same as the run_config_name, but then this is the schema of the table
DQX_CHECKS_LOG_SCHEMA = T.StructType([
    T.StructField("result_id",       T.StringType(),  False),
    T.StructField("rule_id",         T.StringType(),  False),
    T.StructField("source_table",    T.StringType(),  False),
    T.StructField("run_config_name", T.StringType(),  False),
    T.StructField("severity",        T.StringType(),  False),
    T.StructField("name",            T.StringType(),  True),
    T.StructField("message",         T.StringType(),  True),
    T.StructField("columns",         T.ArrayType(T.StringType()), True),
    T.StructField("filter",          T.StringType(),  True),
    T.StructField("function",        T.StringType(),  True),
    T.StructField("run_time",        T.TimestampType(), True),
    T.StructField("user_metadata",   T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("created_by",      T.StringType(),  False),
    T.StructField("created_at",      T.TimestampType(), False),
    T.StructField("updated_by",      T.StringType(),  True),
    T.StructField("updated_at",      T.TimestampType(), True)
])


Then here is more documentation https://databrickslabs.github.io/dqx/docs/guide/quality_checks/#quality-check-results
