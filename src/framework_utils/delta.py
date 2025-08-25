# src/framework_utils/delta.py

"""
Module: utils/deltaConfig.py

Description:
  This module provides the DeltaTableUpdater class, which is responsible for managing
  Delta Lake table operations within a PySpark environment. It facilitates creating Delta tables
  based on a target schema defined in a YAML configuration file (e.g., job-monitor.yaml) and supports
  merging new data into existing Delta tables with precise handling of schema definitions.


Author: Levi Gagne
Created Date: 2025-03-25
Last Modified: 2025-04-16
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, TimestampType, StringType, LongType, DateType, IntegerType,
    DoubleType, FloatType, BooleanType, DecimalType, BinaryType, ShortType, ByteType
)
from typing import Dict, Any, Callable
from delta.tables import DeltaTable


class DeltaTableUpdater:

    def __init__(self, 
                 spark: SparkSession, 
                 table_name: str, 
                 yaml_config: Any,
                 create_if_not_exists: bool = True) -> None:
        """
        Initialize the DeltaTableUpdater instance.
        
        Parameters:
            spark (SparkSession): Active SparkSession for interacting with Spark.
            table_name (str): Fully qualified target table name (e.g., "catalog.schema.table").
            yaml_config (Any): YAML configuration object loaded from a file like job-monitor.yaml containing 
                               schema definitions and app settings.
            create_if_not_exists (bool): Flag to indicate if the table should be automatically created if it does not exist.
        """
        self.spark: SparkSession = spark
        self.table_name: str = table_name
        self.yaml_config = yaml_config
        self.schema: StructType = self.build_target_schema()
        self.partition_column: str = (
            self.yaml_config.config.get("app_config", {})
                .get("plumbing_columns", {})
                .get("dl_partition_column", None)
        )
        if create_if_not_exists:
            self.create_table_if_not_exists()

    def build_target_schema(self) -> StructType:
        """
        Build the target schema from the YAML configuration.
        
        Process:
            - Define a mapping from string representations of types (e.g., "TimestampType")
              as provided in job-monitor.yaml to their corresponding PySpark data types.
            - Iterate through the 'business' and 'plumbing' groups in the YAML configuration's schema section.
            - For each column definition, extract the column name and type string, convert the type
              to a PySpark type using the mapping, and create a corresponding StructField.
            - Aggregate all StructField objects into a StructType.
            
        Returns:
            StructType: The constructed PySpark schema representing the target table.
            
        Raises:
            ValueError: If an unsupported type is encountered in the YAML configuration.
        """

        schema_fields = []

        type_mapping: Dict[str, Callable[..., Any]] = {
            "TimestampType": TimestampType,
            "StringType": StringType,
            "LongType": LongType,
            "DateType": DateType,
            "IntegerType": IntegerType,
            "DoubleType": DoubleType,
            "FloatType": FloatType,
            "BooleanType": BooleanType,
            "DecimalType": DecimalType,
            "BinaryType": BinaryType,
            "ShortType": ShortType,
            "ByteType": ByteType
        }
        for group in ["business", "plumbing"]:
            for col_def in self.yaml_config.config.get("schema", {}).get(group, []):
                col_name = col_def.get("name")
                col_type_name = col_def.get("type")
                if col_type_name not in type_mapping:
                    raise ValueError(
                        f"Unsupported type '{col_type_name}' for column '{col_name}'. "
                        "Please add it to the type_mapping in deltaConfig.py."
                    )
                spark_type = type_mapping[col_type_name]()
                schema_fields.append(StructField(col_name, spark_type, True))
        return StructType(schema_fields)

    def create_table_if_not_exists(self) -> None:
        """
        Create the target Delta table if it does not already exist.
        
        Process:
            - Checks whether the target table exists in the Spark catalog.
            - If the table does not exist, it creates an empty DataFrame with the target schema.
            - Writes the DataFrame as a Delta table.
            - If a partition column (dl_partition_column) is defined in job-monitor.yaml, the table is partitioned by that column.
        """
        if not self.spark.catalog.tableExists(self.table_name):
            empty_df: DataFrame = self.spark.createDataFrame([], schema=self.schema)
            if self.partition_column:
                empty_df.write.format("delta") \
                    .partitionBy(self.partition_column) \
                    .mode("overwrite") \
                    .saveAsTable(self.table_name)
            else:
                empty_df.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(self.table_name)

    def merge_df(self, new_df: DataFrame, merge_condition: str) -> None:
        """
        Merge the provided DataFrame into the existing Delta table.
        
        Process:
            - Restricts the incoming DataFrame to only include columns defined in the target schema.
            - Retrieves the target Delta table by its name.
            - Performs an upsert operation (update if matched, insert if not matched) using the provided SQL merge condition.
              
        Parameters:
            new_df (DataFrame): PySpark DataFrame containing new or updated data.
            merge_condition (str): SQL condition string for matching rows between the target and source DataFrames,
                                   e.g., "target.run_id = source.run_id".
        """
        target_cols = self.schema.fieldNames()
        new_df = new_df.select(*target_cols)
        delta_table: DeltaTable = DeltaTable.forName(self.spark, self.table_name)
        delta_table.alias("target").merge(
            new_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def read_table_as_df(self) -> DataFrame:
        """
        Read the entire Delta table into a PySpark DataFrame.
        
        Returns:
            DataFrame: A PySpark DataFrame representing the contents of the Delta table.
            
        Process:
            - Leverages the SparkSession to retrieve the table using its fully qualified name.
        """
        # Read and return the Delta table as a DataFrame for further analysis.
        return self.spark.table(self.table_name)