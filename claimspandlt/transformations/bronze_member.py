"""
Bronze Member Processing and Consolidation Pipeline.

Consumes mapped EDI stream from upstream DLT tables, enforces structural schemas, 
and executes consolidation transformations.

Target DLT Tables:
    - bronze_member_processed
    - member_consolidated
"""

import os
import sys
import dlt
from pyspark.sql.functions import col, from_json

REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT", 
    os.path.abspath(os.path.join(os.getcwd(), "../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.shared.filestoprocess import FilesToProcess
from src.utils.filehandling import FileHandler
from src.shared.consolidation import ConsolidationProcessor

SCHEMA_PATH = os.path.join(REPO_ROOT, "src/dimmember/bronze/schemas/memberschema.json")
MEMBER_SCHEMA = FileHandler.load_struct_type(SCHEMA_PATH)
CONSOLIDATION_SCHEMA_DIR = os.path.join(REPO_ROOT, "src/dimmember/bronze/schemas/consolidation")


@dlt.table(name="bronze_member_processed")
def bronze_member_processed():
    """Extracts structured 834 member records applying schema validation."""
    parsed_df = (
        dlt.read_stream("edi_parsed_mapped")
        .filter((col("status") == "SUCCESS") & (col("extracted_layout_id") == "834"))
        .select(
            from_json(col("edi_parsed"), MEMBER_SCHEMA).alias("data"),
            col("source_file_path").alias("FILE_ID"),
            col("extracted_client_id").alias("CLIENT_ID"),
            col("extracted_layout_id").alias("FILE_LAYOUT_ID")
        )
        .select("data.*", "FILE_ID", "CLIENT_ID", "FILE_LAYOUT_ID")
    )
    
    return FilesToProcess.process_bronze_member_stream(parsed_df)


@dlt.table(name="member_consolidated")
def member_consolidated():
    """Applies bronze consolidation transformation rules."""
    stream_df = dlt.read_stream("bronze_member_processed")
    
    return ConsolidationProcessor.process_consolidation_stream(
        spark=spark,
        df_stream=stream_df,
        ConsolidatedLayerDataModelFilePath=f"{CONSOLIDATION_SCHEMA_DIR}/datamodels",
        ConsolidatedLayerDataModel="memberdatamodel.json",
        ConsolidatedMappingFilePath=CONSOLIDATION_SCHEMA_DIR,
        ConsolidatedMappingFileName="consolidationmemberschema.json"
    )