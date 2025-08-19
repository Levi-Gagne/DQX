So now im getting this errro: 
RuntimeError: Project root not found (no pyproject.toml or setup.py)
File <command-4730582016129069>, line 804
    791     return main(
    792         output_config_path=dqx_cfg_yaml,
    793         rules_dir=None,
   (...)
    800         created_by=created_by,
    801     )
    803 # ---- run it ----
--> 804 res = load_checks(
    805     dqx_cfg_yaml="resources/dqx_config.yaml",
    806     created_by="AdminUser",
    807     # dry_run=True,
    808     # validate_only=True,
    809     batch_dedupe_mode="warn",
    810 )
    811 print(res)
File /Workspace/Users/levi.gagne@claconnect.com/DQX/src/dqx/utils/path.py:14, in find_project_root(start_path)
     12     if (parent / "pyproject.toml").exists() or (parent / "setup.py").exists():
     13         return parent
---> 14 raise RuntimeError("Project root not found (no pyproject.toml or setup.py)")


I doint even think i call this path function or use

WHy would we use it? why did you integrate it and for what? 



What the actual fuck is your problem? i never asked for this funciton i dont even know what it does


