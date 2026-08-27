import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from src.shared.consolidation import ConsolidationProcessor

class LoopConsolidation:
    """Parses incoming consolidation JSON payloads and coordinates single-file consolidation calls."""

    @staticmethod
    def process_consolidation_json(spark: SparkSession, consolidation_json: str) -> str:
        if not consolidation_json or consolidation_json.strip() == "":
            raise ValueError("ConsolidationJSON input is empty. Please provide a valid JSON string.")

        try:
            parsed_json = json.loads(consolidation_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format provided: {e}")

        file_id_df = spark.createDataFrame([parsed_json])

        exploded_file_ids = file_id_df.select(explode(col("FileIds")).alias("col")).select(
            col("col.DataGroupTrackingID"),
            col("col.DataGroupMappingId"),
            col("col.FileId"),
            col("col.FileLayoutID"),
            col("col.FileLayoutDescription"),
            col("col.CurrentContainer"),
            col("col.CurrentFolderPath"),
            col("col.ConsolidatedMappingFilePath"),
            col("col.ConsolidatedMappingFileName"),
            col("col.ConsolidatedLayerDataModelFilePath"),
            col("col.ConsolidatedLayerDataModel"),
            col("col.ConsolidatedFolderPath")
        )

        rows = exploded_file_ids.collect()
        rJSON_list = []

        for idx, t in enumerate(rows, 1):
            try:
                results = ConsolidationProcessor.process_consolidation(
                    spark=spark,
                    FileId=str(t["FileId"]),
                    CurrentContainer=str(t["CurrentContainer"]),
                    CurrentFolderPath=str(t["CurrentFolderPath"]),
                    ConsolidatedLayerDataModel=str(t["ConsolidatedLayerDataModel"]),
                    ConsolidatedLayerDataModelFilePath=str(t["ConsolidatedLayerDataModelFilePath"]),
                    ConsolidatedMappingFileName=str(t["ConsolidatedMappingFileName"]),
                    ConsolidatedMappingFilePath=str(t["ConsolidatedMappingFilePath"]),
                    ConsolidatedFolderPath=str(t["ConsolidatedFolderPath"])
                )
            except Exception as e:
                results = json.dumps({
                    "CurrentJobId": "None",
                    "ConsolidatedCount": "0",
                    "Status": "FAILED",
                    "ErrorMessage": str(e)
                })

            try:
                res_data = json.loads(results)
            except Exception as parse_err:
                res_data = {
                    "CurrentJobId": "Error",
                    "ConsolidatedCount": "0",
                    "Status": "FAILED",
                    "ErrorMessage": f"Failed to parse JSON: {str(parse_err)}. Raw result: {results[:10]}"
                }

            record = {
                "FileID": t["FileId"],
                "DataGroupTrackingID": t["DataGroupTrackingID"],
                "DataGroupMappingId": t["DataGroupMappingId"],
                "CurrentContainer": t["CurrentContainer"],
                "CurrentFolderPath": t["CurrentFolderPath"],
                "ConsolidatedFolderPath": t["ConsolidatedFolderPath"],
                "CurrentJobId": res_data.get("CurrentJobId", ""),
                "ConsolidatedCount": str(res_data.get("ConsolidatedCount", "0")),
                "Status": res_data.get("Status", ""),
                "ErrorMessage": res_data.get("ErrorMessage", "")
            }
            rJSON_list.append(record)

        return json.dumps(rJSON_list)