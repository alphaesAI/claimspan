import json
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType
)
from pyspark.sql.functions import col, lit, expr

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

    @classmethod
    def get_select_expr_from_list(cls, seq_columns: list) -> list:
        """Build SQL select expressions with type casting directly from Python dictionary list."""
        sql_commands = []
        for row in seq_columns:
            data_type = row.get("DataType", "StringType")
            source_column = row.get("SourceColumn", "").strip().strip("'").strip('"') if row.get("SourceColumn") else ""
            destination_column = row.get("DestinationColumn")
            source_column_format = row.get("SourceColumnFormat", "")
            column_query_raw = row.get("ColumnQuery", "")
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

    @classmethod
    def custom_select(cls, available_cols: list, required_fields_meta: list) -> list:
        """Add null columns for missing fields, explicitly casting to target DataModel SQL types."""
        select_expressions = []
        for item in required_fields_meta:
            field_name = item["FieldName"]
            data_type = item["DataType"]
            
            if field_name in available_cols:
                select_expressions.append(col(field_name))
            else:
                # Explicitly cast NULL to avoid NullType streaming failures
                sql_type = cls.get_sql_type(data_type)
                select_expressions.append(expr(f"CAST(NULL AS {sql_type})").alias(field_name))
                
        return select_expressions

    @classmethod
    def process_consolidation_stream(
        cls,
        spark: SparkSession,
        df_stream: DataFrame,
        ConsolidatedLayerDataModelFilePath: str,
        ConsolidatedLayerDataModel: str,
        ConsolidatedMappingFilePath: str,
        ConsolidatedMappingFileName: str
    ) -> DataFrame:
        """Applies dynamic metadata-driven mapping directly onto a streaming DataFrame."""
        
        data_model_path = os.path.join(ConsolidatedLayerDataModelFilePath, ConsolidatedLayerDataModel)
        mapping_path = os.path.join(ConsolidatedMappingFilePath, ConsolidatedMappingFileName)
        
        # 1. Read metadata files via Driver IO
        with open(data_model_path, 'r') as f:
            data_model_json = json.load(f)
            
        with open(mapping_path, 'r') as f:
            mapping_json = json.load(f)

        # 2. Extract Data Model Fields with Ordinal sorting
        fields_list = data_model_json.get("Fields", [])
        sorted_fields = sorted(fields_list, key=lambda x: x.get("Ordinal", 0))

        # 3. Extract Mapping Columns
        mappings_list = []
        for cmap in mapping_json.get("columnMapping", []):
            mappings_list.extend(cmap.get("selectColumns", []))
            
        mapping_dict = {
            item["DestinationColumn"]: {
                "SourceColumn": item.get("SourceColumn", ""),
                "SourceColumnFormat": item.get("SourceColumnFormat", ""),
                "ColumnQuery": item.get("ColumnQuery", "")
            }
            for item in mappings_list
        }

        # 4. Build sequence list and schema metadata
        seq_columns = []
        required_fields_meta = []
        
        for field_info in sorted_fields:
            field_name = field_info["FieldName"]
            data_type = field_info["DataType"]
            
            required_fields_meta.append({
                "FieldName": field_name,
                "DataType": data_type
            })
            
            if field_name in mapping_dict:
                seq_columns.append({
                    "FieldName": field_name,
                    "DestinationColumn": field_name,
                    "DataType": data_type,
                    "SourceColumn": mapping_dict[field_name]["SourceColumn"],
                    "SourceColumnFormat": mapping_dict[field_name]["SourceColumnFormat"],
                    "ColumnQuery": mapping_dict[field_name]["ColumnQuery"]
                })

        # 5. Transform stream with expressions
        sel_cols = cls.get_select_expr_from_list(seq_columns)
        mapped_df = df_stream.selectExpr(*sel_cols)
        
        # 6. Apply typed NULLs for missing fields
        return mapped_df.select(*cls.custom_select(mapped_df.columns, required_fields_meta))