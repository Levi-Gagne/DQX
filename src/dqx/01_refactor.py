def main(
    output_config_path: str = "resources/dqx_config.yaml",
    rules_dir: Optional[str] = None,
    time_zone: str = "America/Chicago",
    dry_run: bool = False,
    validate_only: bool = False,
    required_fields: Optional[List[str]] = None,
    batch_dedupe_mode: str = "warn",
    table_doc: Optional[Dict[str, Any]] = None,
    created_by: str = "AdminUser",
    apply_table_metadata: bool = False,
):
    spark = SparkSession.builder.getOrCreate()

    # Simple: use the provided time_zone as-is.
    print_notebook_env(spark, local_timezone=time_zone)

    cfg = ProjectConfig(output_config_path, spark=spark)
    rules_dir = rules_dir or cfg.yaml_rules_dir()
    delta_table_name, primary_key = cfg.checks_config_table()

    required_fields = required_fields or ["table_name", "name", "criticality", "run_config_name", "check"]

    # Create-once / metadata gating
    doc = _materialize_table_doc(table_doc or DQX_CHECKS_CONFIG_METADATA, delta_table_name)
    exists = table_exists(spark, delta_table_name)
    if not exists:
        create_table_if_absent(
            spark,
            delta_table_name,
            schema=TABLE_SCHEMA,
            table_doc=doc,
            primary_key=primary_key,
            partition_by=None,
        )
    elif apply_table_metadata:
        apply_table_documentation(spark, delta_table_name, doc)

    # Discover YAML files
    yaml_files = cfg.list_rule_files(rules_dir)
    display_section("YAML FILES DISCOVERED (recursive)")
    files_df = spark.createDataFrame([(p,) for p in yaml_files], "yaml_path string")
    show_df(files_df, n=500, truncate=False)

    if validate_only:
        print("\nValidation only: not writing any rules.")
        errs = validate_rule_files(yaml_files, required_fields)
        return {
            "mode": "validate_only",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "errors": errs,
        }

    # Collect rules
    all_rules: List[dict] = []
    for full_path in yaml_files:
        file_rules = process_yaml_file(
            full_path,
            required_fields=required_fields,
            time_zone=time_zone,
            created_by=created_by
        )
        if file_rules:
            all_rules.extend(file_rules)
            print(f"[loader] {full_path}: rules={len(file_rules)}")

    if not all_rules:
        print("No rules discovered; nothing to do.")
        return {"mode": "no_op", "config_path": output_config_path, "rules_files": len(yaml_files), "wrote_rows": 0}

    print(f"[loader] total parsed rules (pre-dedupe): {len(all_rules)}")

    pre_dedupe = len(all_rules)
    all_rules = dedupe_rules_in_batch_by_check_id(all_rules, mode=batch_dedupe_mode)
    post_dedupe = len(all_rules)

    df = spark.createDataFrame(all_rules, schema=TABLE_SCHEMA)
    debug_display_batch(spark, df)

    unique_check_ids = df.select("check_id").distinct().count()
    distinct_pairs  = df.select("check_id", "run_config_name").distinct().count()

    if dry_run:
        display_section("DRY-RUN: FULL RULES PREVIEW")
        show_df(df.orderBy("table_name", "name"), n=1000, truncate=False)
        return {
            "mode": "dry_run",
            "config_path": output_config_path,
            "rules_files": len(yaml_files),
            "rules_pre_dedupe": pre_dedupe,
            "rules_post_dedupe": post_dedupe,
            "unique_check_ids": unique_check_ids,
            "distinct_rule_run_pairs": distinct_pairs,
            "target_table": delta_table_name,
            "wrote_rows": 0,
            "primary_key": primary_key,
        }

    overwrite_rules_into_delta(spark, df, delta_table_name, table_doc=None, primary_key=primary_key)
    wrote_rows = df.count()
    print(f"{Color.b}{Color.ivory}Finished writing rules to '{Color.r}{Color.b}{Color.i}{Color.sea_green}{delta_table_name}{Color.r}{Color.b}{Color.ivory}' (overwrite){Color.r}.")

    return {
        "mode": "overwrite",
        "config_path": output_config_path,
        "rules_files": len(yaml_files),
        "rules_pre_dedupe": pre_dedupe,
        "rules_post_dedupe": post_dedupe,
        "unique_check_ids": unique_check_ids,
        "distinct_rule_run_pairs": distinct_pairs,
        "target_table": delta_table_name,
        "wrote_rows": wrote_rows,
        "constraint": f"pk_{primary_key}",
        "primary_key": primary_key,
    }