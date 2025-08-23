"""
Module: utils/deltaConfig.py
Description:
  This module provides the DeltaTableUpdater class, which is responsible for managing
  Delta Lake table operations within a PySpark environment. It facilitates creating Delta tables
  based on a target schema defined in a YAML configuration file (e.g., job-monitor.yaml) and supports
  merging new data into existing Delta tables with precise handling of schema definitions.

  Workflow:
  1. Initialization:
     - Instantiates DeltaTableUpdater with an active SparkSession, a fully qualified target table name,
       a YAML configuration object loaded from job-monitor.yaml, and a flag to create the table if it doesn't exist.
     - Invokes the build_target_schema method to construct the target schema using column definitions provided
       in the 'business' and 'plumbing' sections of job-monitor.yaml.
     - Retrieves the partition column by extracting the "dl_partition_column" key from the YAML configuration under
       app_config.plumbing_columns. This value dictates how the table will be partitioned.
     - If specified, automatically creates an empty Delta table with the defined schema.
  2. Schema Building:
     - The build_target_schema method constructs a PySpark StructType by mapping string type names defined in
       job-monitor.yaml (e.g., "TimestampType", "StringType", etc.) to their corresponding PySpark data types.
  3. Table Management:
     - The create_table_if_not_exists method verifies whether the target table exists in the Spark catalog.
       If it does not exist, it creates an empty Delta table using the constructed schema, partitioned by the
       dl_partition_column value as defined in job-monitor.yaml.
  4. Data Merging:
     - The merge_df method merges new data from a DataFrame into the target Delta table using a specified SQL merge condition.
       It ensures that only the columns defined in the target schema are included in the merge operation.
  5. Data Retrieval:
     - The read_table_as_df method reads the entire Delta table into a PySpark DataFrame for further processing.

Usage:
     - Import DeltaTableUpdater from this module and instantiate it with the required parameters.
     
Note:
     - This module is part of the configuration utilities and should be located in the utils folder.

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
    """
    Class: DeltaTableUpdater
    Description:
      Provides a generic updater for Delta tables within a PySpark environment.
      This class is responsible for:
         - Creating a Delta table if it does not exist,
         - Building the table schema from a YAML configuration file (e.g., job-monitor.yaml),
         - Merging new data into the table based on a specified merge condition,
         - Reading the Delta table into a DataFrame.
      
      The YAML configuration should include a schema section organized in 'business' and 'plumbing'
      groups and an application configuration section containing the dl_partition_column key.
      
    Workflow:
      - Initialization: Saves the Spark session and table name, builds the schema, extracts the
        partitioning column from the YAML configuration, and creates the table if requested.
      - Schema Building: Uses the build_target_schema method to map YAML data types to PySpark types.
      - Table Management: Invokes create_table_if_not_exists to create an empty Delta table partitioned
        by dl_partition_column from job-monitor.yaml if the table does not exist.
      - Data Merging: The merge_df method performs an upsert by merging new records with existing ones.
      - Data Retrieval: The read_table_as_df method loads the entire table into a DataFrame.
    """
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
            
        Process:
            - Retains the Spark session and table name.
            - Constructs the target schema using build_target_schema.
            - Retrieves the partition column from the YAML config under app_config.plumbing_columns.dl_partition_column.
            - Optionally creates the Delta table if it does not exist.
        """
        # Store the provided SparkSession for table operations.
        self.spark: SparkSession = spark
        # Store the table name to be used throughout the updater.
        self.table_name: str = table_name
        # Save the YAML configuration, which contains schema and application settings.
        self.yaml_config = yaml_config
        # Build the table schema from the YAML configuration.
        self.schema: StructType = self.build_target_schema()
        # Extract the partition column (dl_partition_column) from the YAML configuration.
        self.partition_column: str = (
            self.yaml_config.config.get("app_config", {})      # Access app_config section
                .get("plumbing_columns", {})                   # Access plumbing_columns subsection
                .get("dl_partition_column", None)              # Retrieve dl_partition_column value if exists
        )
        # Create the table if the flag is True and the table does not exist.
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
        # Initialize an empty list to store Spark StructField objects.
        schema_fields = []
        # Define mapping from YAML string names to PySpark type constructors.
        type_mapping: Dict[str, Callable[..., Any]] = {
            "TimestampType": TimestampType,
            "StringType": StringType,
            "LongType": LongType,
            "DateType": DateType,
            "IntegerType": IntegerType,
            "DoubleType": DoubleType,
            "FloatType": FloatType,
            "BooleanType": BooleanType,
            "DecimalType": DecimalType,   # May require precision and scale parameters.
            "BinaryType": BinaryType,
            "ShortType": ShortType,
            "ByteType": ByteType
        }
        # Process schema definitions from both 'business' and 'plumbing' sections.
        for group in ["business", "plumbing"]:
            # Get the list of column definitions for the current group.
            for col_def in self.yaml_config.config.get("schema", {}).get(group, []):
                # Retrieve the column name.
                col_name = col_def.get("name")
                # Retrieve the string representation of the column type.
                col_type_name = col_def.get("type")
                # Check if the type from YAML is supported.
                if col_type_name not in type_mapping:
                    # If the type is unsupported, raise an error with a descriptive message.
                    raise ValueError(
                        f"Unsupported type '{col_type_name}' for column '{col_name}'. "
                        "Please add it to the type_mapping in deltaConfig.py."
                    )
                # Create a PySpark type instance using the mapping.
                spark_type = type_mapping[col_type_name]()
                # Append a new StructField with the column name, type, and nullable flag set to True.
                schema_fields.append(StructField(col_name, spark_type, True))
        # Combine all StructField objects into a single StructType and return it.
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
        # Check if the table already exists using Spark's catalog interface.
        if not self.spark.catalog.tableExists(self.table_name):
            # Create an empty DataFrame with the defined schema.
            empty_df: DataFrame = self.spark.createDataFrame([], schema=self.schema)
            # If a partition column is specified, partition the table based on that column.
            if self.partition_column:
                empty_df.write.format("delta") \
                    .partitionBy(self.partition_column) \
                    .mode("overwrite") \
                    .saveAsTable(self.table_name)           # Save DataFrame as a Delta table.
            else:
                # If no partition column is specified, simply write the DataFrame as a Delta table.
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
        # Retrieve the list of column names as defined in the target schema.
        target_cols = self.schema.fieldNames()
        # Select only the columns from new_df that are present in the target schema.
        new_df = new_df.select(*target_cols)
        # Access the Delta table using its fully qualified table name.
        delta_table: DeltaTable = DeltaTable.forName(self.spark, self.table_name)
        # Merge new data into the Delta table:
        #   - If rows match the merge_condition, update all columns.
        #   - If rows do not match, insert them into the table.
        delta_table.alias("target").merge(
            new_df.alias("source"),    # Alias the source DataFrame for merge operations.
            merge_condition            # Specify the SQL merge condition.
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