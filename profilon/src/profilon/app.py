# app.py
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="DQX Rule Generator", layout="wide")  # no emoji icon

HTML = """
<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans"; }
    .container { max-width: 1100px; margin: 40px auto; }
    .header { padding: 24px 28px; border: 1px solid #e5e7eb; border-radius: 10px; background: #0b1220; color: #e5e7eb; }
    .title { font-size: 28px; font-weight: 700; margin: 0 0 8px 0; color: #d1e7ff; }
    .subtitle { color: #9fb4cc; font-size: 14px; margin: 0; }
    .card { margin-top: 18px; padding: 18px 20px; border: 1px solid #e5e7eb; border-radius: 10px; background: #fff; }
    .h2 { margin: 0 0 10px 0; font-size: 18px; color: #111827; }
    .p { color: #374151; font-size: 14px; line-height: 1.6; margin: 0 0 8px 0; }
    .list { margin: 8px 0 0 18px; color: #374151; font-size: 14px; }
    code { background: #0b1220; color: #c7d0e0; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">DQX Rule Generator</div>
      <div class="subtitle">Configure profiling & rule generation, save to a Volume, and trigger a Databricks Job to build checks.</div>
    </div>

    <div class="card">
      <div class="h2">How it works</div>
      <p class="p">Use the <b>Configure & Run</b> page to choose targets, profile options, and output settings.</p>
      <ul class="list">
        <li>Writes a config YAML to your chosen <b>Volume</b> (overwrites).</li>
        <li>Triggers a Databricks <b>Notebook Job</b> with <code>CONFIG_PATH</code> param (via widget).</li>
        <li>Your existing code reads YAML, runs Spark, and writes DQX checks (YAML files or table rows).</li>
      </ul>
    </div>

    <div class="card">
      <div class="h2">Why widgets?</div>
      <p class="p">Notebook tasks receive per-run parameters via <b>widgets</b>. It’s just a key–value channel from Jobs — no UI interaction required.</p>
      <p class="p">If you want real CLI args, switch to a Python Wheel task later.</p>
    </div>

    <div class="card">
      <div class="h2">Next steps</div>
      <p class="p">Open the sidebar and navigate to <b>Configure & Run</b>.</p>
    </div>
  </div>
</body>
</html>
"""

components.html(HTML, height=600, scrolling=True)
