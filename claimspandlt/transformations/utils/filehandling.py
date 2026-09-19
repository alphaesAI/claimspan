import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class FileHandler:
    """Parses JSON schema files directly into PySpark StructTypes."""
    
    @staticmethod
    def load_struct_type(schema_json_path: str) -> StructType:
        """Reads custom MemberSchema.json format and returns a PySpark StructType."""
        with open(schema_json_path, "r") as f:
            schema_data = json.load(f)
            
        fields = []
        # Support both 'columnNames' array and standard json schema arrays
        column_list = schema_data.get("columnNames", schema_data)
        
        for col_def in column_list:
            field_name = col_def.get("col", {}).get("FieldName") or col_def.get("FieldName")
            if field_name and field_name != "TEMPLATE":
                fields.append(StructField(field_name.strip(), StringType(), True))
                
        return StructType(fields)
    
def load_json_config(config_path: str) -> dict:
    """Loads and returns a JSON configuration file securely."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def resolve_path_placeholders(path_str: str, catalog_name: str) -> str:
    """Replaces placeholders like #clientCode dynamically with the active catalog/container."""
    if not path_str:
        return ""
    return path_str.replace("#clientCode", catalog_name)

def read_source_dataset(spark: SparkSession, source_table: str, source_format: str):
    """Dynamically reads a source dataframe whether it's a catalog table or file path."""
    if "." in source_table and not source_table.startswith('/'):
        return spark.table(source_table)
    else:
        return spark.read.format(source_format).option("header", "true").option("inferSchema", "true").load(source_table)

def execute_sql_template(spark: SparkSession, sql_script_path: str, base_dir: str, catalog_name: str, temp_view_name: str = "temp_updates"):
    """Reads an external SQL script file, replaces variables, executes it, and registers a temp view."""
    sql_path_abs = os.path.normpath(os.path.join(base_dir, sql_script_path))
    with open(sql_path_abs, "r", encoding="utf-8") as sf:
        sql_query = sf.read().replace("#clientCode", catalog_name)
    
    df = spark.sql(sql_query)
    df.createOrReplaceTempView(temp_view_name)
    return df