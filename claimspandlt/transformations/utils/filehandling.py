import json
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