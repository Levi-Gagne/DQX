streamlit==1.38.0
databricks-sdk>=0.30
PyYAML>=6.0
# If you decide to query a Warehouse directly (optional):
# databricks-sql-connector>=3.0

#####



command: [
  "streamlit",
  "run",
  "src/profilon/app.py",
  "--server.headless", "true",
  "--browser.gatherUsageStats", "false"
]