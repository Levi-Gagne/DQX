# pages/1_Configure_&_Run.py
import os
import io
import time
import uuid
import yaml
import json
import hashlib
import streamlit as st
from typing import List, Dict, Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

st.set_page_config(page_title="Configure & Run", layout="wide")

# ---------- Unity Catalog discovery via SDK ----------
def safe_list_catalogs(wc: WorkspaceClient) -> List[str]:
    try:
        return [c.name for c in wc.catalogs.list() if isinstance(c, CatalogInfo)]
    except Exception as e:
        st.info(f"Catalog list unavailable ({e}); type values manually.")
        return []

def safe_list_schemas(wc: WorkspaceClient, catalog: str) -> List[str]:
    try:
        return [s.name for s in wc.schemas.list(catalog_name=catalog) if isinstance(s, SchemaInfo)]
    except Exception as e:
        st.info(f"Schema list unavailable ({e}); type values manually.")
        return []

def safe_list_tables(wc: WorkspaceClient, catalog: str, schema: str) -> List[str]:
    try:
        return [t.name for t in wc.tables.list(catalog_name=catalog, schema_name=schema) if isinstance(t, TableInfo)]
    except Exception as e:
        st.info(f"Table list unavailable ({e}); type values manually.")
        return []

def safe_list_columns(wc: WorkspaceClient, catalog: str, schema: str, table: str) -> List[str]:
    try:
        t = wc.tables.get(full_name=f"{catalog}.{schema}.{table}")
        cols = getattr(t, "columns", None) or []
        return [c.name for c in cols]
    except Exception as e:
        st.info(f"Column list unavailable ({e}); you can continue without column selection.")
        return []

# ---------- small utils ----------
def validate_volume_path(p: str) -> None:
    if not p or not p.startswith("/Volumes/"):
        raise ValueError("Path must start with /Volumes/<catalog>/<schema>/<volume>/...")

def save_text_overwrite(path: str, text: str) -> None:
    validate_volume_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def render_yaml_with_optional_block(cfg: Dict[str, Any]) -> str:
    base = yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False).rstrip() + "\n"
    optional = """
# --- Optional / documented historically; keep for reference ---
# profile_options:
#   include_histograms: false
#   min_length: null
#   max_length: null
#   min_value: null
#   max_value: null
#   profile_types: null
""".lstrip("\n")
    return base + optional

def job_run_now_with_retry(wc: WorkspaceClient, job_id: int, params: Dict[str, str], attempts: int = 3, backoff: float = 1.5):
    last_err = None
    for i in range(attempts):
        try:
            return wc.jobs.run_now(job_id=job_id, notebook_params=params)
        except Exception as e:
            last_err = e
            time.sleep(backoff ** i)
    raise last_err

# ---------- Sidebar: job trigger ----------
with st.sidebar:
    st.caption("Workspace actions")
    job_id = st.text_input("Databricks Job ID", placeholder="1234567890 (required to run)")
    run_button = st.button("Run Job (Save + Trigger)", use_container_width=True)

wc = WorkspaceClient()  # default auth in Databricks Apps

# ---------- Rule Generator options ----------
st.title("Configure & Run")

c1, c2 = st.columns(2)
with c1:
    mode = st.selectbox("Mode", ["table", "schema", "catalog", "pipeline"], index=0)
    output_format = st.selectbox("Output Format", ["yaml", "table"], index=0)
    created_by = st.text_input("Created By", value="LMG")
    run_config_name = st.text_input("Run Config Name", value="default")
    criticality = st.selectbox("Criticality", ["warn", "error"], index=0)
    include_table_name = True  # fixed
    st.write("YAML Key Order: **custom** (fixed)")

with c2:
    if output_format == "yaml":
        output_location = st.text_input(
            "Output Location (YAML path)",
            value="/Volumes/<catalog>/<schema>/<volume>/dqx_checks",
            help="Directory or full YAML file path. Will OVERWRITE."
        )
    else:
        output_location = st.text_input(
            "Output Location (table FQN)",
            value="dq_prd.monitoring.dqx_checks_table",
            help="Fully qualified: catalog.schema.table (rules appended)."
        )

st.markdown("<hr/>", unsafe_allow_html=True)

# ---------- Target selection ----------
st.subheader("Target Selection")
exclude_pattern = st.text_input("Exclude pattern (optional, e.g. '.tmp_*')", value="")

name_param = ""
columns: Optional[List[str]] = None

if mode == "pipeline":
    name_param = st.text_input("Pipelines (CSV)", value="my_pipeline_1,my_pipeline_2")

elif mode == "catalog":
    catalogs = safe_list_catalogs(wc)
    catalog = st.selectbox("Catalog", catalogs) if catalogs else st.text_input("Catalog", value="dq_prd")
    name_param = catalog

elif mode == "schema":
    catalogs = safe_list_catalogs(wc)
    catalog = st.selectbox("Catalog", catalogs) if catalogs else st.text_input("Catalog", value="dq_prd")
    schemas = safe_list_schemas(wc, catalog) if catalogs else []
    schema = st.selectbox("Schema", schemas) if schemas else st.text_input("Schema", value="monitoring")
    name_param = f"{catalog}.{schema}"

else:  # table
    catalogs = safe_list_catalogs(wc)
    catalog = st.selectbox("Catalog", catalogs) if catalogs else st.text_input("Catalog", value="dq_prd")
    schemas = safe_list_schemas(wc, catalog) if catalogs else []
    schema = st.selectbox("Schema", schemas) if schemas else st.text_input("Schema", value="monitoring")
    tables = safe_list_tables(wc, catalog, schema) if schemas else []
    table = st.selectbox("Table", tables) if tables else st.text_input("Table", value="job_run_audit")
    name_param = f"{catalog}.{schema}.{table}"

    all_cols = safe_list_columns(wc, catalog, schema, table) if (tables or table) else []
    columns = st.multiselect("Columns (optional; default = all)", options=all_cols, default=[]) or None

st.markdown("<hr/>", unsafe_allow_html=True)

# ---------- Profile Options ----------
st.subheader("Profile Options")

pc1, pc2, pc3 = st.columns(3)
with pc1:
    sample_fraction = st.slider("sample_fraction", min_value=0.0, max_value=1.0, value=0.3, step=0.05)
    sample_seed = st.number_input("sample_seed (0 = None)", value=0, step=1, min_value=0)
    limit = st.number_input("limit", min_value=0, value=1000, step=100)

with pc2:
    remove_outliers = st.selectbox("remove_outliers", [False, True], index=0)
    num_sigmas = st.slider("num_sigmas", min_value=1.0, max_value=5.0, value=3.0, step=0.5)
    trim_strings = st.selectbox("trim_strings", [True, False], index=0)
    round_values = st.selectbox("round (min/max rounding)", [True, False], index=0)

with pc3:
    max_null_ratio = st.slider("max_null_ratio", min_value=0.0, max_value=1.0, value=0.05, step=0.01)
    max_empty_ratio = st.slider("max_empty_ratio", min_value=0.0, max_value=1.0, value=0.02, step=0.01)
    distinct_ratio = st.slider("distinct_ratio", min_value=0.0, max_value=1.0, value=0.01, step=0.01)
    max_in_count = st.number_input("max_in_count", min_value=1, value=20, step=1)

profile_options: Dict[str, Any] = {
    "sample_fraction": float(sample_fraction),
    "sample_seed": None if int(sample_seed) == 0 else int(sample_seed),
    "limit": int(limit),

    "remove_outliers": bool(remove_outliers),
    "outlier_columns": [],  # add selection later if needed
    "num_sigmas": float(num_sigmas),

    "max_null_ratio": float(max_null_ratio),
    "trim_strings": bool(trim_strings),
    "max_empty_ratio": float(max_empty_ratio),

    "distinct_ratio": float(distinct_ratio),
    "max_in_count": int(max_in_count),

    "round": bool(round_values),

    # ---- preserved but commented in YAML output ----
    # "include_histograms": False,
    # "min_length": None,
    # "max_length": None,
    # "min_value": None,
    # "max_value": None,
    # "profile_types": None,
}

# ---------- Build config dict + fingerprint ----------
def build_config_dict() -> Dict[str, Any]:
    created_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    cfg: Dict[str, Any] = {
        "mode": mode,
        "name_param": name_param,
        "output_format": output_format,
        "output_location": output_location,
        "profile_options": profile_options,
        "exclude_pattern": exclude_pattern or None,
        "created_by": created_by or "unknown",
        "columns": columns,
        "run_config_name": run_config_name,
        "criticality": criticality,
        "yaml_key_order": "custom",
        "include_table_name": True,
        "_snapshot": {
            "created_at": created_at,
            "created_by": created_by or "unknown",
            "app": "dqx-app",
            "run_uuid": str(uuid.uuid4()),
        },
    }
    raw = yaml.safe_dump(cfg, sort_keys=True, default_flow_style=False)
    cfg["_snapshot"]["fingerprint_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return cfg

cfg = build_config_dict()
preview_yaml = render_yaml_with_optional_block(cfg)

st.subheader("Save / Run")
config_path = st.text_input(
    "Config YAML path (Volume file)",
    value="/Volumes/<catalog>/<schema>/<volume>/dqx_rulegen_config.yaml",
    help="Will be OVERWRITTEN on Save / Run."
)
st.code(preview_yaml, language="yaml")

colA, colB = st.columns([1,1])
with colA:
    if st.button("💾 Save YAML", use_container_width=True):
        try:
            save_text_overwrite(config_path, preview_yaml)
            st.success(f"Saved config to: {config_path}")
        except Exception as e:
            st.error(f"Failed to save config: {e}")

with colB:
    if run_button:
        if not job_id.strip().isdigit():
            st.error("Enter a numeric Job ID in the sidebar.")
        else:
            try:
                # one-two action: save then trigger
                save_text_overwrite(config_path, preview_yaml)
                run = job_run_now_with_retry(
                    wc, job_id=int(job_id), params={"CONFIG_PATH": config_path}, attempts=3
                )
                st.success(f"Saved and triggered job_id={job_id}. Run ID: {getattr(run, 'run_id', 'N/A')}")
            except Exception as e:
                st.error(f"Failed to trigger job: {e}")
