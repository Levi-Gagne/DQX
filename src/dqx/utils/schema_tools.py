# src/dqx/utils/schema_tools.py
from __future__ import annotations
from typing import Dict, Any, Optional
from pyspark.sql import types as T

__all__ = ["struct_to_columns_spec"]

def _dtype_to_name(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType): return "string"
    if isinstance(dt, T.BooleanType): return "boolean"
    if isinstance(dt, T.TimestampType): return "timestamp"
    if isinstance(dt, T.DateType): return "date"
    if isinstance(dt, T.LongType): return "long"
    if isinstance(dt, T.IntegerType): return "integer"
    if isinstance(dt, T.ShortType): return "short"
    if isinstance(dt, T.ByteType): return "byte"
    if isinstance(dt, T.FloatType): return "float"
    if isinstance(dt, T.DoubleType): return "double"
    if isinstance(dt, T.DecimalType): return f"decimal({dt.precision},{dt.scale})"
    if isinstance(dt, T.ArrayType): return "array"
    if isinstance(dt, T.MapType): return "map"
    if isinstance(dt, T.StructType): return "struct"
    return "string"

def _field_spec_from_struct_field(sf: T.StructField) -> Dict[str, Any]:
    dt = sf.dataType
    base: Dict[str, Any] = {
        "name": sf.name,
        "data_type": _dtype_to_name(dt),
        "nullable": bool(sf.nullable),
    }
    # carry comment from metadata, if present
    try:
        meta_comment = (sf.metadata or {}).get("comment")
        if meta_comment:
            base["comment"] = meta_comment
    except Exception:
        pass

    if isinstance(dt, T.StructType):
        fields_spec: Dict[str, Any] = {}
        for i, child in enumerate(dt.fields, start=1):
            fields_spec[f"field_{i}"] = _field_spec_from_struct_field(child)
        base["fields"] = fields_spec

    elif isinstance(dt, T.ArrayType):
        elem = dt.elementType
        elem_spec: Dict[str, Any] = {"type": _dtype_to_name(elem)}
        if isinstance(elem, T.StructType):
            sub: Dict[str, Any] = {}
            for i, child in enumerate(elem.fields, start=1):
                sub[f"field_{i}"] = _field_spec_from_struct_field(child)
            elem_spec["fields"] = sub
        base["element"] = elem_spec

    elif isinstance(dt, T.MapType):
        base["key_type"]   = _dtype_to_name(dt.keyType)
        base["value_type"] = _dtype_to_name(dt.valueType)

    return base

def struct_to_columns_spec(struct: T.StructType, *, comments: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Convert a Spark StructType into the columns-spec dict consumed by TableWriter.
    - Preserves nullable + (meta) comments from StructField.metadata['comment']
    - Optional `comments` overrides per column name
    """
    cols: Dict[str, Any] = {}
    for i, sf in enumerate(struct.fields, start=1):
        spec = _field_spec_from_struct_field(sf)
        if comments and sf.name in comments:
            spec["comment"] = comments[sf.name]
        cols[f"column_{i}"] = spec
    return cols