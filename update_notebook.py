import json
import sys

with open("ClaimsProcessing/dimProvider/provider_pipeline.ipynb", "r") as f:
    notebook = json.load(f)

for cell in notebook.get("cells", []):
    if cell.get("cell_type") == "code":
        source = "".join(cell.get("source", []))
        if "def build_payloads(" in source:
            new_source = """def build_payloads(processed_files: list) -> tuple:
    \"\"\"Generates precise production payloads for downstream orchestration stages.\"\"\"
    process_list = []
    consolidation_list = []
    
    for f in processed_files:
        layout_id = f['layout_id']
        specific_client_container = f"{ROOT_DIR}/temp/{layout_id}"
        
        if layout_id == '274':
            schema_name = "provider_hierarchy_schema.json"
            processed_folder = "/Volumes/claimspan/bronze/provider_hierarchy"
            consol_folder = "/Volumes/claimspan/bronze/provider_hierarchy_consolidated"
            mapping_id = f"{layout_id}PROVIDER_HIERARCHY"
            consol_mapping_name = "ConsolidationProviderHierarchy.json"
            consol_data_model = "ProviderHierarchyDataModel.json"
        else:
            schema_name = "provider_schema.json"
            processed_folder = "/Volumes/claimspan/bronze/provider"
            consol_folder = "/Volumes/claimspan/bronze/provider_consolidated"
            mapping_id = f"{layout_id}PROVIDER"
            consol_mapping_name = "ConsolidationProvider.json"
            consol_data_model = "ProviderDataModel.json"
        
        process_list.append({
            "ClientID": f['client_id'],
            "FileID": f['file_id'],
            "FileName": f['csv_filename'],
            "ClientContainer": specific_client_container,
            "CurrentFolderPath": "",
            "ProcessedFolderPath": processed_folder,
            "ColumnDelimiter": ",",
            "HasHeader": "true",
            "IgnoreHeader": "False",
            "FileLayoutID": layout_id,
            "FileLayoutDescription": f"Standard{layout_id}",
            "SchemaFileName": schema_name,
            "SchemaFilePath": f"{ROOT_DIR}/dimProvider/Bronze/Schema",
            "TextQualifier": "\\\""
        })
        
        consolidation_list.append({
            "DataGroupTrackingID": f"TRACK_PROVIDER_{layout_id}_{f['file_id']}",
            "DataGroupMappingId": mapping_id,
            "FileId": f['file_id'],
            "FileLayoutID": layout_id,
            "FileLayoutDescription": f"Standard{layout_id}",
            "CurrentContainer": processed_folder,
            "CurrentFolderPath": "",
            "ConsolidatedMappingFilePath": f"{ROOT_DIR}/dimProvider/Bronze/Schema/Consolidation",
            "ConsolidatedMappingFileName": consol_mapping_name,
            "ConsolidatedLayerDataModelFilePath": f"{ROOT_DIR}/dimProvider/Bronze/Schema/Consolidation/DataModels",
            "ConsolidatedLayerDataModel": consol_data_model,
            "ConsolidatedFolderPath": consol_folder
        })
        
    return json.dumps({"FileIds": process_list}), json.dumps({"FileIds": consolidation_list})"""
            # Need to format as list of lines, ending with \n
            lines = [line + '\n' for line in new_source.split('\n')]
            # Remove trailing newline from the very last line
            if lines:
                lines[-1] = lines[-1][:-1]
            cell["source"] = lines

with open("ClaimsProcessing/dimProvider/provider_pipeline.ipynb", "w") as f:
    json.dump(notebook, f, indent=1)
