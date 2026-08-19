import json
import os
import ast

# Load mappings
with open('ClaimsProcessing/dimProvider/EDIProcessing/mappings.py', 'r') as f:
    source = f.read()

# Parse AST
tree = ast.parse(source)
mappings_dict = None
for node in tree.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == 'MAPPINGS':
                mappings_dict = node.value

if mappings_dict is None:
    raise ValueError("MAPPINGS not found")

expressions = None
for key, value in zip(mappings_dict.keys, mappings_dict.values):
    if isinstance(key, ast.Constant) and key.value == 'expressions':
        expressions = value
        break

fields_274 = []
fields_837 = []

for key in expressions.keys:
    if isinstance(key, ast.Constant):
        k = key.value
        if k == "TEMPLATE":
            continue
        if k.isupper() or k.startswith("TIER"):
            fields_274.append(k)
        else:
            fields_837.append(k)

# Add TEMPLATE to schemas
def create_schema(fields):
    schema = {
        "columnNames": [
            {
                "FieldName": "TEMPLATE",
                "OrdinalPosition": -1,
                "DataType": "TEMPLATE",
                "Format": "TEMPLATE",
                "Validators": [
                    {
                        "Name": "TEMPLATE",
                        "Value": "TEMPLATE",
                        "InputValue": ["TEMPLATE"],
                        "ConditionCol": "TEMPLATE",
                        "ConditionVal": ["TEMPLATE"]
                    }
                ]
            }
        ],
        "Rules": [
            {
                "Name": "VerifyTotalCount",
                "Data_file_query": "select count(1) + 1 from data_file",
                "control_file_query": "select _c7 from control_file"
            },
            {
                "Name": "VerifyTotalDataCount",
                "Data_file_query": "select count(1) from data_file",
                "control_file_query": "select _c8 from control_file"
            }
        ]
    }
    
    for idx, field in enumerate(fields):
        schema["columnNames"].append({
            "FieldName": field,
            "OrdinalPosition": idx,
            "DataType": "string"
        })
    return schema

def create_consolidation(fields):
    column_mapping = []
    
    for field in fields:
        column_mapping.append({
            "SourceColumn": field,
            "DestinationColumn": field,
            "SourceColumnFormat": "",
            "ColumnQuery": ""
        })
        
    # Append standard file metadata columns
    for col in ["FILE_ID", "FILE_LAYOUT_ID", "FILE_LAYOUT_DESCRIPTION", "CLIENT_ID", "LOAD_DATETIME"]:
        column_mapping.append({
            "SourceColumn": col,
            "DestinationColumn": col,
            "SourceColumnFormat": "",
            "ColumnQuery": ""
        })
        
    return {
        "columnMapping": [
            {
                "recordType": [{"Field": "", "Value": ""}],
                "selectColumns": column_mapping
            }
        ]
    }

def create_datamodel(fields):
    datamodel_fields = []
    
    ordinal = 1
    # Standard metadata fields
    for col in ["ClientID", "FileID", "LoadDateTime", "FileLayoutID", "FileLayoutDescription"]:
        datamodel_fields.append({
            "FieldName": col,
            "DataType": "StringType",
            "Ordinal": ordinal
        })
        ordinal += 1
        
    for field in fields:
        datamodel_fields.append({
            "FieldName": field,
            "DataType": "StringType",
            "Ordinal": ordinal
        })
        ordinal += 1
        
    return {"Fields": datamodel_fields}

# Generate 837 schema
schema_837 = create_schema(fields_837)
consol_837 = create_consolidation(fields_837)
data_837 = create_datamodel(fields_837)

schema_274 = create_schema(fields_274)
consol_274 = create_consolidation(fields_274)
data_274 = create_datamodel(fields_274)

# Write files
base_dir = "ClaimsProcessing/dimProvider/Bronze/Schema"
consol_dir = os.path.join(base_dir, "Consolidation")
model_dir = os.path.join(consol_dir, "DataModels")

os.makedirs(consol_dir, exist_ok=True)
os.makedirs(model_dir, exist_ok=True)

# 837
with open(os.path.join(base_dir, "provider_schema.json"), "w") as f:
    json.dump(schema_837, f, indent=2)

with open(os.path.join(consol_dir, "ConsolidationProvider.json"), "w") as f:
    json.dump(consol_837, f, indent=2)
    
with open(os.path.join(model_dir, "ProviderDataModel.json"), "w") as f:
    json.dump(data_837, f, indent=2)

# 274
# Let's write the consolidation files for 274
with open(os.path.join(consol_dir, "ConsolidationProviderHierarchy.json"), "w") as f:
    json.dump(consol_274, f, indent=2)
    
with open(os.path.join(model_dir, "ProviderHierarchyDataModel.json"), "w") as f:
    json.dump(data_274, f, indent=2)

print("Generated all schema and consolidation files successfully.")
