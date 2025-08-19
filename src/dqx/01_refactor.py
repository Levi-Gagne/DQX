So i just got this error: 
[runtime] Non-fatal while printing runtime info: Invalid timezone 'America/Chicago'. Must be a valid IANA timezone string. Examples: 'UTC', 'America/Chicago', 'Europe/Berlin'.


NameError: name '__file__' is not defined
File <command-4730582016129069>, line 782
    769     return main(
    770         output_config_path=dqx_cfg_yaml,
    771         rules_dir=None,
   (...)
    778         created_by=created_by,
    779     )
    781 # ---- run it ----
--> 782 res = load_checks(
    783     dqx_cfg_yaml="resources/dqx_config.yaml",
    784     created_by="AdminUser",
    785     # dry_run=True,
    786     # validate_only=True,
    787     batch_dedupe_mode="warn",
    788 )
    789 print(res)
File <command-4730582016129069>, line 673, in main(output_config_path, rules_dir, time_zone, dry_run, validate_only, required_fields, batch_dedupe_mode, table_doc, created_by)
    670 spark = SparkSession.builder.getOrCreate()
    671 print_notebook_env(spark, local_timezone=time_zone)
--> 673 cfg = ProjectConfig(output_config_path, spark=spark, start_file=__file__)
    674 rules_dir = rules_dir or cfg.yaml_rules_dir()
    675 delta_table_name, primary_key = cfg.checks_config_table()
