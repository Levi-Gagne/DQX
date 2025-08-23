# src/dqx/utils/write.py
from __future__ import annotations
from typing import Dict, Iterable, List, Optional

from pyspark.sql import SparkSession, DataFrame, types as T


class TableWriter:
    """
    Minimal, Spark-first table creator and DataFrame writer.

    - Caller must pass StructType, format, and (optionally) partition_by.
    - No schema alignment/reordering: Spark/UC errors naturally if mismatched.
    - All metadata (comments, tags, properties, PK) applied inside create_table().
    - Primary key name is AUTO-GENERATED; you only pass the PK column(s).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ---------- public: CREATE ----------
    def create_table(
        self,
        *,
        fqn: str,
        schema: T.StructType,
        format: str,
        options: Optional[Dict[str, str]] = None,
        partition_by: Optional[Iterable[str]] = None,
        # metadata (all optional)
        table_comment: Optional[str] = None,
        column_comments: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
        table_tags: Optional[Dict[str, str]] = None,
        column_tags: Optional[Dict[str, Dict[str, str]]] = None,  # {"col": {"tag":"val", ...}, ...}
        primary_key_cols: Optional[Iterable[str]] = None,         # auto-name is derived
    ) -> None:
        """
        Create an empty table with the given schema and apply metadata.

        Required:
          - fqn, schema (StructType), format.

        Notes:
          - partition_by is honored only at CREATE time.
          - If primary_key_cols is provided, we enforce NOT NULL on those columns
            then add the primary key constraint with an auto-generated name.
        """
        self._require(fqn, "fqn")
        self._require_struct(schema)
        self._require(format, "format")

        # 1) CREATE empty table from StructType
        df = self.spark.createDataFrame([], schema)
        writer = df.write.format(format)  # <-- no mode on create
        if options:
            writer = writer.options(**options)
        if partition_by:
            parts = [partition_by] if isinstance(partition_by, str) else list(partition_by)
            if parts:
                writer = writer.partitionBy(*parts)
        writer.saveAsTable(fqn)

        # 2) Apply metadata
        if table_comment:
            self.set_table_comment(fqn, table_comment)
        if column_comments:
            self.set_column_comments(fqn, column_comments)
        if table_properties:
            self.set_table_properties(fqn, table_properties)
        if table_tags:
            self.set_table_tags(fqn, table_tags)
        if column_tags:
            self.set_column_tags(fqn, column_tags)

        # 3) Primary key (NOT NULL first, then ADD CONSTRAINT with auto name)
        if primary_key_cols:
            cols = [primary_key_cols] if isinstance(primary_key_cols, str) else list(primary_key_cols)
            if not cols:
                raise ValueError("primary_key_cols must not be empty when provided.")
            self._enforce_not_null(fqn, cols)
            self._add_primary_key_auto(fqn, cols)

    # ---------- public: WRITE ----------
    def write_df(
        self,
        df: DataFrame,
        *,
        fqn: str,
        mode: str,
        format: str,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Simple table write. No alignment, no reordering, no defaults.
        """
        self._require(fqn, "fqn")
        self._require(mode, "mode")
        self._require(format, "format")

        w = df.write.format(format).mode(mode)
        if options:
            w = w.options(**options)
        w.saveAsTable(fqn)

    # ---------- public: metadata primitives ----------
    def set_table_comment(self, fqn: str, comment: str) -> None:
        self._sql(f"COMMENT ON TABLE {self._qt(fqn)} IS '{self._esc(comment)}'")

    def set_column_comments(self, fqn: str, comments: Dict[str, str]) -> None:
        if not comments:
            return
        base = self._qt(fqn)
        for col, text in comments.items():
            self._sql(f"COMMENT ON COLUMN {base}.{self._qpath(col)} IS '{self._esc(text)}'")

    def set_table_properties(self, fqn: str, props: Dict[str, str]) -> None:
        if not props:
            return
        assigns = ", ".join([f"'{k}'='{self._esc(v)}'" for k, v in props.items()])
        self._sql(f"ALTER TABLE {self._qt(fqn)} SET TBLPROPERTIES ({assigns})")

    def set_table_tags(self, fqn: str, tags: Dict[str, str]) -> None:
        if not tags:
            return
        assigns = ", ".join([f"'{k}' = '{self._esc(v)}'" for k, v in tags.items()])
        self._sql(f"ALTER TABLE {self._qt(fqn)} SET TAGS ({assigns})")

    def set_column_tags(self, fqn: str, tags_by_col: Dict[str, Dict[str, str]]) -> None:
        if not tags_by_col:
            return
        base = self._qt(fqn)
        for col, tag_map in tags_by_col.items():
            if not tag_map:
                continue
            assigns = ", ".join([f"'{k}' = '{self._esc(v)}'" for k, v in tag_map.items()])
            self._sql(f"ALTER TABLE {base} ALTER COLUMN {self._qid(col)} SET TAGS ({assigns})")

    # ---------- internals (PK) ----------
    def _enforce_not_null(self, fqn: str, cols: List[str]) -> None:
        base = self._qt(fqn)
        for c in cols:
            self._sql(f"ALTER TABLE {base} ALTER COLUMN {self._qid(c)} SET NOT NULL")

    def _add_primary_key_auto(self, fqn: str, cols: List[str]) -> None:
        base = self._qt(fqn)
        table = fqn.split(".")[-1].strip("`")
        suffix = cols[0] if len(cols) == 1 else f"{len(cols)}cols"
        name = f"pk_{table}_{suffix}"
        col_list = ", ".join(self._qid(c) for c in cols)
        self._sql(f"ALTER TABLE {base} ADD CONSTRAINT {self._qid(name)} PRIMARY KEY ({col_list})")

    # ---------- tiny utils ----------
    @staticmethod
    def _require(value: Optional[str], label: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required argument: {label}")

    @staticmethod
    def _require_struct(schema: T.DataType) -> None:
        if not isinstance(schema, T.StructType):
            raise TypeError("schema must be a pyspark.sql.types.StructType")

    def _sql(self, statement: str) -> None:
        self.spark.sql(statement)

    @staticmethod
    def _esc(s: str) -> str:
        return (s or "").replace("'", "''")

    @staticmethod
    def _qid(ident: str) -> str:
        return f"`{ident.strip('`')}`"

    def _qt(self, fqn: str) -> str:
        return ".".join(self._qid(p) for p in fqn.split("."))

    def _qpath(self, path: str) -> str:
        # Supports nested paths for comments (e.g., "parent.child")
        return ".".join(self._qid(p) for p in path.split("."))