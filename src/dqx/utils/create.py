# src/dqx/utils/create.py
from __future__ import annotations

from typing import List, Tuple, Optional, Dict, Any, Union
from pyspark.sql import SparkSession, DataFrame, types as T
from utils.sqlfmt import q_fqn, esc_comment


class TableCreator:
    """
    Create-time utilities (no existence checks here).
    - Build StructType from column specs (name, dtype, nullable, comment)
    - Create empty Delta table (optionally partitioned)
    - Apply table/column comments once (from YAML table_description + field metadata)
    - Add PRIMARY KEY once at create-time
    """

    # ---------- Schema builders ----------
    @staticmethod
    def build_schema_from_specs(specs: List[Tuple[str, T.DataType, bool, str]]) -> T.StructType:
        fields: List[T.StructField] = []
        for name, dtype, nullable, comment in specs:
            meta = {"comment": comment} if comment else {}
            fields.append(T.StructField(name, dtype, nullable, meta))
        return T.StructType(fields)

    @staticmethod
    def _empty_df_from_schema(spark: SparkSession, schema: T.StructType) -> DataFrame:
        return spark.createDataFrame([], schema)

    # ---------- Create empty Delta ----------
    @staticmethod
    def _write_empty_delta_table(
        spark: SparkSession,
        table_fqn: str,
        schema: T.StructType,
        partition_by: Optional[List[str]] = None,
    ) -> None:
        df = TableCreator._empty_df_from_schema(spark, schema)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table_fqn)

    # ---------- Documentation (applied ONLY at create) ----------
    @staticmethod
    def _build_doc_from_schema(
        table_description: Optional[str],
        schema: T.StructType,
        *,
        metadata_key: str = "comment",
    ) -> Dict[str, Any]:
        cols: Dict[str, str] = {}
        for f in schema.fields:
            try:
                meta = f.metadata or {}
                comment = meta.get(metadata_key)
                if isinstance(comment, str) and comment.strip():
                    cols[f.name] = comment
            except Exception:
                pass
        return {"table_comment": table_description or "", "columns": cols}

    @staticmethod
    def _apply_table_documentation(spark: SparkSession, table_fqn: str, doc: Dict[str, Any]) -> None:
        if not doc:
            return
        qtable = q_fqn(table_fqn)

        # Table comment
        table_comment = esc_comment(doc.get("table_comment", ""))
        if table_comment:
            try:
                spark.sql(f"COMMENT ON TABLE {qtable} IS '{table_comment}'")
            except Exception:
                spark.sql(f"ALTER TABLE {qtable} SET TBLPROPERTIES ('comment' = '{table_comment}')")

        # Column comments (best-effort)
        cols: Dict[str, str] = doc.get("columns", {}) or {}
        existing = {f.name.lower() for f in spark.table(table_fqn).schema.fields}
        for col_name, comment in cols.items():
            if col_name.lower() not in existing:
                continue
            qcol = f"`{col_name}`"
            stmt = f"ALTER TABLE {qtable} ALTER COLUMN {qcol} COMMENT '{esc_comment(comment)}'"
            try:
                spark.sql(stmt)
            except Exception:
                try:
                    spark.sql(f"COMMENT ON COLUMN {qtable}.{qcol} IS '{esc_comment(comment)}'")
                except Exception as e2:
                    print(f"[doc] Skipped column comment for {table_fqn}.{col_name}: {e2}")

    # ---------- Constraint ----------
    @staticmethod
    def _add_primary_key_constraint(
        spark: SparkSession,
        table_fqn: str,
        column: Union[str, List[str]] = "check_id",
    ) -> None:
        qtable = q_fqn(table_fqn)
        if isinstance(column, (list, tuple)):
            cols_sql = ", ".join(f"`{c}`" for c in column)
            cols_label = ", ".join(str(c) for c in column)
        else:
            cols_sql = f"`{column}`"
            cols_label = str(column)
        sql = f"ALTER TABLE {qtable} ADD PRIMARY KEY ({cols_sql})"
        try:
            spark.sql(sql)
            print(f"[pk] PRIMARY KEY set on {table_fqn}: ({cols_label})")
        except Exception as e:
            print(f"[pk] Unable to set PRIMARY KEY on {table_fqn}: {e}")

    # ---------- Public API (no existence checks) ----------
    @staticmethod
    def create_table(
        spark: SparkSession,
        table_fqn: str,
        *,
        column_specs: List[Tuple[str, T.DataType, bool, str]],
        partition_by: Optional[List[str]] = None,
        primary_key: Optional[Union[str, List[str]]] = "check_id",
        table_description: Optional[str] = None,
    ) -> None:
        """
        Unconditionally creates the empty Delta table with the exact schema,
        applies documentation once, and adds PRIMARY KEY once.
        Callers are responsible for gating (checking existence, flags, etc.).
        """
        schema = TableCreator.build_schema_from_specs(column_specs)
        TableCreator._write_empty_delta_table(spark, table_fqn, schema, partition_by=partition_by)

        # Apply documentation once
        doc = TableCreator._build_doc_from_schema(table_description, schema)
        if doc:
            try:
                TableCreator._apply_table_documentation(spark, table_fqn, doc)
            except Exception as e:
                print(f"[doc] Failed to apply documentation at create: {e}")

        # Add PK once
        if primary_key:
            TableCreator._add_primary_key_constraint(spark, table_fqn, column=primary_key)

    @staticmethod
    def apply_metadata(
        spark: SparkSession,
        table_fqn: str,
        *,
        column_specs: List[Tuple[str, T.DataType, bool, str]],
        table_description: Optional[str] = None,
    ) -> None:
        """
        Apply/refresh documentation (table + columns) on an already-existing table.
        Does NOT alter schema, partitioning, or constraints.
        """
        schema = TableCreator.build_schema_from_specs(column_specs)
        doc = TableCreator._build_doc_from_schema(table_description, schema)
        TableCreator._apply_table_documentation(spark, table_fqn, doc)