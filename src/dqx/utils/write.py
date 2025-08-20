# src/dqx/utils/write.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------- Errors ----------------
class WriteConfigError(RuntimeError):
    pass

# ---------------- Validation dictionaries ----------------
ALLOWED_WRITE_MODES = {"append", "overwrite", "ignore", "errorifexists"}
ALLOWED_TABLE_FORMATS = {"delta", "parquet", "csv", "json", "orc"}  # extend if you actually use more

# ---------------- Tiny helpers ----------------
_SIMPLE_TYPES = {
    # integers
    "byte": ByteType(),
    "short": ShortType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    # floats
    "float": FloatType(),
    "double": DoubleType(),
    # bool/strings/binary
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "string": StringType(),
    "binary": BinaryType(),
    # dates/times
    "date": DateType(),
    "timestamp": TimestampType(),
}
_DECIMAL_RE = re.compile(r"^decimal\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE)

def _parse_scalar_dtype(name: str) -> DataType:
    t = (name or "").strip().lower()
    if t in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[t]
    m = _DECIMAL_RE.match(t)
    if m:
        from pyspark.sql.types import DecimalType
        return DecimalType(int(m.group(1)), int(m.group(2)))
    raise WriteConfigError(f"Unsupported scalar data_type: {name!r}")

def _ordered_pos_keys(d: Dict[str, Any], prefix: str) -> List[str]:
    def keyer(k: str) -> int:
        try:
            if k.startswith(prefix + "_"):
                return int(k.split("_", 1)[1])
        except Exception:
            pass
        return 10**9
    return sorted(d.keys(), key=keyer)

def _q_ident(ident: str) -> str:
    return ".".join(f"`{part.replace('`','``')}`" for part in ident.split("."))

def _q_lit(s: Optional[str]) -> str:
    v = "" if s is None else str(s)
    return "'" + v.replace("'", "''") + "'"

def _ensure_str_list(name: str, v: Optional[List[Any]]) -> List[str]:
    if v is None:
        return []
    if not isinstance(v, list) or any(not isinstance(x, str) or not x.strip() for x in v):
        raise WriteConfigError(f"{name} must be a list of non-empty strings.")
    return [x.strip() for x in v]

def _validate_mode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m not in ALLOWED_WRITE_MODES:
        raise WriteConfigError(f"Invalid write mode {mode!r}. Allowed: {sorted(ALLOWED_WRITE_MODES)}")
    return m

def _validate_format(fmt: str) -> str:
    f = (fmt or "").strip().lower()
    if f not in ALLOWED_TABLE_FORMATS:
        raise WriteConfigError(f"Invalid table format {fmt!r}. Allowed: {sorted(ALLOWED_TABLE_FORMATS)}")
    return f

def _validate_options(opts: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if opts is None:
        return {}
    if not isinstance(opts, dict):
        raise WriteConfigError("write options must be a mapping (dict).")
    return opts

# ============================================================
#                         TableWriter
# ============================================================
class TableWriter:
    """
    Decoupled writer for Spark/Delta-like tables.

    - Build StructType from column specs (positional columns/fields)
    - Create empty table if missing; honor partition_by
    - Optional metadata: table comment, column comments, tags (as TBLPROPERTIES)
    - Write DataFrames with validated mode/format/options
    - Optional OPTIMIZE Z-ORDER (manual)

    NOTE:
      * Primary key is stored as a table property (portable); add real constraints later if desired.
      * Arrays/structs/maps are supported via data_type: array|struct|map.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ---------- Schema building (+ spec validation) ----------
    def schema_from_columns(self, columns: Dict[str, Any]) -> StructType:
        if not isinstance(columns, dict) or not columns:
            raise WriteConfigError("columns must be a non-empty mapping")

        fields: List[StructField] = []
        for ck in _ordered_pos_keys(columns, "column"):
            spec = columns[ck]
            if not isinstance(spec, dict):
                raise WriteConfigError(f"{ck} must be a mapping")
            name = (spec.get("name") or "").strip()
            if not name:
                raise WriteConfigError(f"{ck} missing 'name'")
            dt = self._dtype_from_spec(spec)
            nullable = bool(spec.get("nullable", True))
            fields.append(StructField(name, dt, nullable))
        return StructType(fields)

    def _dtype_from_spec(self, spec: Dict[str, Any]) -> DataType:
        kind = (spec.get("data_type") or "").strip().lower()

        # scalar (incl. decimal)
        if kind in _SIMPLE_TYPES or kind.startswith("decimal("):
            return _parse_scalar_dtype(kind)

        # struct
        if kind == "struct":
            fields_node = spec.get("fields") or {}
            if not isinstance(fields_node, dict) or not fields_node:
                raise WriteConfigError("struct requires 'fields' mapping (field_1..N)")
            ordered = _ordered_pos_keys(fields_node, "field")
            sfields: List[StructField] = []
            for fk in ordered:
                f = fields_node[fk]
                if not isinstance(f, dict):
                    raise WriteConfigError(f"{fk} must be a mapping")
                fname = (f.get("name") or "").strip()
                if not fname:
                    raise WriteConfigError(f"{fk} missing 'name'")
                fnull = bool(f.get("nullable", True))
                fdt = self._dtype_from_spec(f)
                sfields.append(StructField(fname, fdt, fnull))
            return StructType(sfields)

        # array
        if kind == "array":
            elem = spec.get("element") or {}
            if not isinstance(elem, dict):
                raise WriteConfigError("array requires 'element' mapping")
            etype = (elem.get("type") or "").strip().lower()
            if not etype:
                raise WriteConfigError("array requires element.type")
            return ArrayType(self._dtype_from_spec({"data_type": etype, **elem}), containsNull=True)

        # map (scalar K/V)
        if kind == "map":
            kt = (spec.get("key_type") or "").strip().lower()
            vt = (spec.get("value_type") or "").strip().lower()
            if not kt or not vt:
                raise WriteConfigError("map requires key_type and value_type")
            return MapType(_parse_scalar_dtype(kt), _parse_scalar_dtype(vt), valueContainsNull=True)

        raise WriteConfigError(f"Unsupported data_type: {kind!r}")

    # ---------- Existence ----------
    def table_exists(self, fqn: str) -> bool:
        try:
            return self.spark.catalog.tableExists(fqn)
        except Exception:
            try:
                self.spark.sql(f"DESCRIBE TABLE { _q_ident(fqn) }")
                return True
            except Exception:
                return False

    # ---------- Creation (idempotent wrapper) ----------
    def create_table(
        self,
        *,
        fqn: str,
        columns: Dict[str, Any],
        format: str = "delta",
        options: Optional[Dict[str, Any]] = None,
        partition_by: Optional[List[str]] = None,
        primary_key: Optional[str] = None,
        apply_metadata: bool = True,
        table_comment: Optional[str] = None,
        table_tags: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Create table if missing (managed). Returns True if created, False if existed.
        Always syncs primary_key property. Applies docs/tags only when apply_metadata=True.
        """
        fmt = _validate_format(format)
        opts = _validate_options(options)
        parts = _ensure_str_list("partition_by", partition_by)

        created = False
        if not self.table_exists(fqn):
            schema = self.schema_from_columns(columns)
            # create with empty DF
            df0 = self.spark.createDataFrame([], schema)
            writer = df0.write.format(fmt).mode("ignore")
            if opts:
                writer = writer.options(**opts)
            if parts:
                writer = writer.partitionBy(*parts)
            writer.saveAsTable(fqn)
            created = True

        # structure-aligned property
        if primary_key:
            self.set_table_properties(fqn, {"primary_key": primary_key})

        # optional metadata
        if apply_metadata:
            self.set_table_comment(fqn, table_comment)
            self.set_column_comments(fqn, columns)
            props = {}
            props.update(self._props_from_table_tags(table_tags))
            props.update(self._props_from_column_tags(columns))
            if props:
                self.set_table_properties(fqn, props)

        return created

    # ---------- Write (rows only) ----------
    def write_only(
        self,
        df: DataFrame,
        *,
        fqn: str,
        mode: str = "append",
        format: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        m = _validate_mode(mode)
        fmt = _validate_format(format)
        opts = _validate_options(options)
        writer = df.write.format(fmt).mode(m)
        if opts:
            writer = writer.options(**opts)
        writer.saveAsTable(fqn)

    # ---------- Optional: manual Z-ORDER ----------
    def optimize_zorder(self, fqn: str, zorder_by: Optional[List[str]]) -> None:
        cols = _ensure_str_list("zorder_by", zorder_by)
        if not cols:
            return
        col_expr = ", ".join(_q_ident(c) for c in cols)
        self.spark.sql(f"OPTIMIZE { _q_ident(fqn) } ZORDER BY ({col_expr})")

    # ---------- Metadata helpers ----------
    def set_table_comment(self, fqn: str, comment_markdown: Optional[str]) -> None:
        if not comment_markdown:
            return
        try:
            self.spark.sql(f"COMMENT ON TABLE { _q_ident(fqn) } IS {_q_lit(comment_markdown)}")
        except Exception:
            self.spark.sql(
                f"ALTER TABLE { _q_ident(fqn) } SET TBLPROPERTIES('comment'={_q_lit(comment_markdown)})"
            )

    def set_column_comments(self, fqn: str, columns: Dict[str, Any]) -> None:
        if not columns:
            return
        for ck in _ordered_pos_keys(columns, "column"):
            spec = columns[ck]
            name = spec.get("name")
            comment = spec.get("comment")
            if name and comment:
                try:
                    self.spark.sql(
                        f"ALTER TABLE { _q_ident(fqn) } ALTER COLUMN { _q_ident(name) } COMMENT {_q_lit(comment)}"
                    )
                except Exception:
                    # Portability: some catalogs require full CHANGE COLUMN; skip if unsupported.
                    pass

    def set_table_properties(self, fqn: str, props: Dict[str, str]) -> None:
        if not props:
            return
        kv = ", ".join(f"{_q_lit(k)}={_q_lit(v)}" for k, v in props.items())
        self.spark.sql(f"ALTER TABLE { _q_ident(fqn) } SET TBLPROPERTIES ({kv})")

    def _props_from_table_tags(self, table_tags: Optional[Dict[str, Any]]) -> Dict[str, str]:
        if not table_tags:
            return {}
        out: Dict[str, str] = {}
        for _, item in sorted(table_tags.items()):
            if not isinstance(item, dict):
                continue
            k = (item.get("key") or "").strip()
            v = (item.get("value") or "").strip()
            if k:
                out[f"tag.{k}"] = v
        return out

    def _props_from_column_tags(self, columns: Optional[Dict[str, Any]]) -> Dict[str, str]:
        if not columns:
            return {}
        props: Dict[str, str] = {}
        for spec in columns.values():
            col = (spec.get("name") or "").strip()
            if not col:
                continue
            ctags = spec.get("column_tag") or {}
            for _, item in sorted(ctags.items()):
                if not isinstance(item, dict):
                    continue
                k = (item.get("key") or "").strip()
                v = (item.get("value") or "").strip()
                if k:
                    props[f"coltag.{col}.{k}"] = v
        return props