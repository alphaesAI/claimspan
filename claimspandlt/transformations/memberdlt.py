import json
import os
import sys
import dlt
from pyspark.sql.functions import col, udf, expr, from_json

from pyspark.sql.types import StringType, StructType, StructField

# Worker path setup
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

# Import helper modules
from src.shared.filestoprocess import FilesToProcess
from src.utils.filehandling import FileHandler
from src.shared.consolidation import ConsolidationProcessor
from src.dimember.silver.membergrouping import ProcessMemberBridge
from src.dimember.silver.member import process_silver_member

SCHEMA_PATH = os.path.join(REPO_ROOT, "src/dimember/schemas/memberschema.json")
MEMBER_SCHEMA = FileHandler.load_struct_type(SCHEMA_PATH)

CONSOLIDATION_SCHEMA_DIR = os.path.join(REPO_ROOT, "ClaimsProcessing/DimMember/Bronze/Schema/Consolidation")

# UDF 1: Raw Segment Extraction
@udf(returnType=StringType())
def extract_edi_json(file_path: str) -> str:
    if not file_path:
        return None
    if REPO_ROOT not in sys.path:
        sys.path.append(REPO_ROOT)
    try:
        from ClaimsProcessing.Shared.EDIProcessing.ediprocessing import EDIProcessor
        processor = EDIProcessor()
        return json.dumps(processor(file_path))
    except Exception as e:
        return json.dumps({"error": str(e)})


# UDF 2: Data Mapping
@udf(returnType=StringType())
def map_edi_json(raw_json_str: str) -> str:
    if not raw_json_str:
        return None
    if REPO_ROOT not in sys.path:
        sys.path.append(REPO_ROOT)
    try:
        raw_json = json.loads(raw_json_str)
        if "error" in raw_json:
            return None
        from ClaimsProcessing.DimMember.EDIProcessing.mapper import Mapper
        mapper = Mapper()
        return json.dumps(mapper.map_member(raw_json))
    except Exception as e:
        return json.dumps({"error": str(e)})


# Step 1: Ingest Raw Files
@dlt.table(name="stg_edi_files")
def stg_edi_files():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile") 
        .load("/Volumes/claimspan/source/834")
        .select(
            col("_metadata.file_path").alias("source_file_path"),
            col("_metadata.file_modification_time").alias("ingested_at")
        )
    )


# Step 2: Intermediate Table - Store Extracted EDI JSON
@dlt.table(name="int_edi_extracted_json")
def int_edi_extracted_json():
    return (
        dlt.read_stream("stg_edi_files")
        .select(
            col("source_file_path"),
            col("ingested_at"),
            extract_edi_json(col("source_file_path")).alias("raw_extracted_json")
        )
    )


# Step 3: Final Table - Map Data & Include Row Status
@dlt.table(name="edi_parsed_mapped")
def edi_parsed_mapped():
    return (
        dlt.read_stream("int_edi_extracted_json")
        .select(
            col("source_file_path"),
            col("ingested_at"),
            map_edi_json(col("raw_extracted_json")).alias("edi_parsed")
        )
        # Explicit status column to identify empty/failed parses (like smithog.txt)
        .withColumn(
            "status",
            expr("CASE WHEN edi_parsed = '{}' OR edi_parsed IS NULL THEN 'FAILED' ELSE 'SUCCESS' END")
        )
    )

@dlt.table(name="bronze_member_processed")
def bronze_member_processed():
    parsed_df = (
        dlt.read_stream("edi_parsed_mapped")
        .filter(col("status") == "SUCCESS")
        .select(
            from_json(col("edi_parsed"), MEMBER_SCHEMA).alias("data"),
            col("source_file_path").alias("FILE_ID")
        )
        .select("data.*", "FILE_ID")
    )
    return FilesToProcess.process_bronze_member_stream(parsed_df)

# Step 5: Dynamic Streaming Consolidation
@dlt.table(name="member_consolidated")
def member_consolidated():
    stream_df = dlt.read_stream("bronze_member_processed")
    
    return ConsolidationProcessor.process_consolidation_stream(
        spark=spark,
        df_stream=stream_df,
        ConsolidatedLayerDataModelFilePath=f"{CONSOLIDATION_SCHEMA_DIR}/DataModels",
        ConsolidatedLayerDataModel="MemberDataModel.json",
        ConsolidatedMappingFilePath=CONSOLIDATION_SCHEMA_DIR,
        ConsolidatedMappingFileName="ConsolidationMember.json"
    )

# Step 6: Silver Member Person Bridge Table
@dlt.table(
    name="silver_memberpersonbridge",
    comment="Silver Member Person Bridge Table processed via DLT"
)
def silver_memberpersonbridge():
    # dlt.read fetches the batch DataFrame snapshot so toPandas() works smoothly
    df_consolidated = dlt.read("member_consolidated")
    return ProcessMemberBridge(df_consolidated)

# Step 7: Final Silver Member Table (Materialized View)
@dlt.table(
    name="silver_member",
    comment="Final Silver Member table joining consolidated member data with person bridge"
)
def silver_member():
    # Fetch batch snapshots of both upstream tables
    df_consolidated = dlt.read("member_consolidated")
    df_person_bridge = dlt.read("silver_memberpersonbridge")
    
    # Execute transformations and return final DataFrame
    return process_silver_member(
        spark=spark,
        df_consolidated=df_consolidated,
        df_person_bridge=df_person_bridge
    )




