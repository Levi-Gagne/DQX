# ======================================================================
# file: src/dqx/utils/dqx.py
# ======================================================================

from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional

from pyspark.sql import DataFrame, functions as F, types as T
from databricks.labs.dqx.engine import DQEngine

# --------- Issue array schema helpers ----------
_ISSUE_STRUCT = T.StructType([
    T.StructField("name",          T.StringType(),  True),
    T.StructField("message",       T.StringType(),  True),
    T.StructField("columns",       T.ArrayType(T.StringType()), True),
    T.StructField("filter",        T.StringType(),  True),
    T.StructField("function",      T.StringType(),  True),
    T.StructField("run_time",      T.TimestampType(), True),
    T.StructField("user_metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("check_id",      T.StringType(),  True),
])

def empty_issues_array() -> F.Column:
    return F.from_json(F.lit("[]"), T.ArrayType(_ISSUE_STRUCT))

def normalize_issues_for_fp(arr_col: F.Column) -> F.Column:
    # stable fingerprint: sort 'columns'; drop run_time/user_metadata/check_id
    return F.transform(
        arr_col,
        lambda r: F.struct(
            r["name"].alias("name"),
            r["message"].alias("message"),
            F.coalesce(F.to_json(F.array_sort(r["columns"])), F.lit("[]")).alias("columns_json"),
            r["filter"].alias("filter"),
            r["function"].alias("function"),
        ),
    )

# --------- Rule application with isolation ----------
def apply_rules_isolating_failures(
    dq: DQEngine, src: DataFrame, rules: List[dict], table_name: str
) -> Tuple[Optional[DataFrame], List[Tuple[str, str]]]:
    try:
        return dq.apply_checks_by_metadata(src, rules), []
    except Exception:
        bad: List[Tuple[str, str]] = []
        good: List[dict] = []
        for r in rules:
            try:
                dq.apply_checks_by_metadata(src, [r])
                good.append(r)
            except Exception as ex:
                bad.append((r.get("name") or "<unnamed>", str(ex)))
        if not good:
            return None, bad
        try:
            return dq.apply_checks_by_metadata(src, good), bad
        except Exception:
            return None, bad

# --------- Argument coercion (uses EXPECTED spec supplied by caller) ----------
def _parse_scalar(s: Optional[str]):
    if s is None: return None
    s = s.strip(); sl = s.lower()
    if sl in ("null","none",""): return None
    if sl == "true": return True
    if sl == "false": return False
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try:
            import json; return json.loads(s)
        except Exception:
            return s
    try:
        return int(s) if s.lstrip("+-").isdigit() else float(s)
    except Exception:
        return s

def _to_list(v):
    if v is None: return []
    if isinstance(v, list): return v
    if isinstance(v, str) and v.strip().startswith("["):
        try:
            import json; return json.loads(v)
        except Exception:
            return [v]
    return [v]

def _to_num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return v
    try: return int(v) if str(v).lstrip("+-").isdigit() else float(v)
    except Exception: return v

def _to_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str):
        vl = v.strip().lower()
        if vl in ("true","t","1"): return True
        if vl in ("false","f","0"): return False
    return v

def coerce_arguments(
    expected_spec: Dict[tuple, Dict[str, str]] | Dict[str, Dict[str, str]],
    args_map: Optional[Dict[str, str]],
    function_name: Optional[str],
    *,
    mode: str = "permissive",
) -> Tuple[Dict[str, Any], List[str]]:
    if not args_map: return {}, []
    raw = {k:_parse_scalar(v) for k, v in args_map.items()}
    spec = expected_spec.get((function_name or "").strip(), {}) if isinstance(expected_spec, dict) else {}

    out: Dict[str, Any] = {}; errs: List[str] = []
    for k, v in raw.items():
        want = spec.get(k)
        if want == "list":
            out[k] = _to_list(v)
            if not isinstance(out[k], list): errs.append(f"key '{k}' expected list, got {type(out[k]).__name__}")
        elif want == "num":
            out[k] = _to_num(v)
        elif want == "bool":
            out[k] = _to_bool(v)
        elif want == "str":
            out[k] = "" if v is None else str(v)
        else:
            out[k] = v

    if mode == "strict" and errs:
        raise ValueError(f"Argument coercion failed for '{function_name}': {errs}")
    return out, errs