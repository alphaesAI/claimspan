import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType
)
from pyspark.sql.functions import col, lit, explode

class ConsolidationProcessor:
    """Encapsulates data consolidation logic, column mapping, and dynamic schema alignment."""

    @staticmethod
    def get_sql_type(data_type: str) -> str:
        """Convert DataModel data type to SQL type string."""
        type_mapping = {
            "StringType": "STRING",
            "IntegerType": "INT",
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE",
            "FloatType": "FLOAT",
            "BooleanType": "BOOLEAN",
            "DateType": "DATE",
            "TimestampType": "TIMESTAMP",
            "DecimalType": "DECIMAL(38,10)"
        }
        return type_mapping.get(data_type, "STRING")

    @staticmethod
    def get_data_type(data_type: str):
        """Convert DataModel data type to PySpark DataType."""
        type_mapping = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "LongType": LongType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),   
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "DecimalType": DecimalType(38, 10)
        }
        return type_mapping.get(data_type, StringType())

    @classmethod
    def get_struct(cls, data_model_df: DataFrame) -> StructType:
        """Create StructType schema from DataModel DataFrame."""
        fields = []
        for row in data_model_df.orderBy("Ordinal").collect():
            field_name = row.FieldName
            data_type = cls.get_data_type(row.DataType)
            fields.append(StructField(field_name, data_type, nullable=True))
        return StructType(fields)

    @classmethod
    def get_select_expr(cls, cols_df: DataFrame) -> list:
        """Build SQL select expressions with type casting."""
        sql_commands = []
        for row in cols_df.collect():
            field_name = row.FieldName
            data_type = row.DataType
            source_column = row.SourceColumn.strip().strip("'").strip('"') if row.SourceColumn else ""
            destination_column = row.DestinationColumn
            source_column_format = row.SourceColumnFormat if row.SourceColumnFormat else ""
            column_query_raw = row.ColumnQuery if row.ColumnQuery else ""
            column_query = column_query_raw.strip().strip("'").strip('"') if column_query_raw else ""
            
            if column_query and column_query.strip():
                expr_str = f"NULLIF(CAST({column_query} AS {cls.get_sql_type(data_type)}),'') AS {destination_column}"
            elif data_type == "DateType" and source_column_format:
                expr_str = f"to_date({source_column},'{source_column_format}') AS {destination_column}"
            elif data_type == "TimestampType" and source_column_format:
                expr_str = f"to_timestamp({source_column},'{source_column_format}') AS {destination_column}"
            else:
                expr_str = f"CAST({source_column} AS {cls.get_sql_type(data_type)}) AS {destination_column}"
            
            sql_commands.append(expr_str)
        return sql_commands

    @staticmethod
    def custom_select(available_cols: list, required_cols: list) -> list:
        """Add null columns for missing fields from DataModel."""
        return [
            col(column) if column in available_cols else lit(None).alias(column)
            for column in required_cols
        ]

    @classmethod
    def process_consolidation(
        cls,
        spark: SparkSession,
        FileId: str,
        CurrentContainer: str,
        CurrentFolderPath: str,
        ConsolidatedLayerDataModel: str,
        ConsolidatedLayerDataModelFilePath: str,
        ConsolidatedMappingFileName: str,
        ConsolidatedMappingFilePath: str,
        ConsolidatedFolderPath: str
    ) -> str:
        """Process a single file consolidation."""
        rJSON = {}
        double_quote = '"'
        
        try:
            current_job_id = "Undefined"
            rJSON["CurrentJobId"] = current_job_id
            
            FullProcessed = f"{CurrentContainer}/{CurrentFolderPath}/"
            FullConsolidatedFolderPath = ConsolidatedFolderPath
            DataModelFile = f"{ConsolidatedLayerDataModelFilePath}/{ConsolidatedLayerDataModel}"
            ConsolidationMapping = f"{ConsolidatedMappingFilePath}/{ConsolidatedMappingFileName}"
            
            # Load DataModel JSON
            temp_data_model = spark.read.format("json").option("multiline", "true").load(DataModelFile)
            data_model = temp_data_model.select(explode(col("Fields"))).select(
                col("col.FieldName").alias("FieldName"),
                col("col.DataType").alias("DataType"),
                col("col.Ordinal").alias("Ordinal")
            )
            
            # Create destination schema
            dest_schema = cls.get_struct(data_model)
            df_data_model = spark.createDataFrame([], dest_schema)
            
            # Load ConsolidationMapping file
            consolidated_mappings = spark.read.format("json").option("multiline", "true").load(ConsolidationMapping)
            temp_mappings = consolidated_mappings.select(explode(col("columnMapping"))).select(
                col("col.recordType").alias("recordType"),
                col("col.selectColumns").alias("selectColumns")
            )
            
            s_record_type = temp_mappings.select(explode(col("recordType"))).select(
                col("col.Field").alias("Field"),
                col("col.Value").alias("Value")
            )
            s_columns = temp_mappings.select(explode(col("selectColumns"))).select(
                col("col.SourceColumn").alias("SourceColumn"),
                col("col.DestinationColumn").alias("DestinationColumn"),
                col("col.SourceColumnFormat").alias("SourceColumnFormat"),
                col("col.ColumnQuery").alias("ColumnQuery")
            )
            
            seq_columns = data_model.join(
                s_columns,
                data_model["FieldName"] == s_columns["DestinationColumn"],
                "inner"
            ).select(
                data_model["FieldName"],
                data_model["DataType"],
                data_model["Ordinal"],
                s_columns["SourceColumn"],
                s_columns["DestinationColumn"],
                s_columns["SourceColumnFormat"],
                s_columns["ColumnQuery"]
            )
            
            sel_cols = cls.get_select_expr(seq_columns)
            
            record_type_row = s_record_type.select("Field", "Value").collect()[-1]
            filter_field = "FileId" if not record_type_row.Field or record_type_row.Field == "" else record_type_row.Field
            filter_value = FileId if not record_type_row.Value or record_type_row.Value == "" else record_type_row.Value
            
            df_file_reformatted = spark.read.format("parquet").load(FullProcessed) \
                .selectExpr(*sel_cols) \
                .filter(col("FileID") == FileId) \
                .filter(col(filter_field) == filter_value)
            
            df_file = df_data_model.union(
                df_file_reformatted.select(
                    *cls.custom_select(
                        df_file_reformatted.columns,
                        df_data_model.columns
                    )
                )
            )
            
            record_count = df_file.count()
            
            if record_count > 0:
                df_file.write.format("delta").option("mergeSchema", "true").mode("append").save(FullConsolidatedFolderPath)
            
            rJSON["ConsolidatedCount"] = str(record_count)
            rJSON["Status"] = "SUCCESS"
            rJSON["ErrorMessage"] = ""
            
        except Exception as e:
            error_msg = str(e).replace(double_quote, "").replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()
            error_msg = ''.join(char for char in error_msg if ord(char) >= 32)
            
            rJSON["ConsolidatedCount"] = "0"
            rJSON["Status"] = "FAILED"
            rJSON["ErrorMessage"] = error_msg
        
        return json.dumps(rJSON)