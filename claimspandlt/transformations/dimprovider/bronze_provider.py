"""
Bronze Provider Processing and Consolidation Pipeline.

Consumes mapped EDI stream from upstream DLT tables, enforces structural schemas, 
and executes consolidation transformations for provider data (837 claims with provider info).

Target DLT Tables:
    - bronze_provider_processed
    - provider_consolidated
    - bronze_provider_hierarchy_processed
    - provider_hierarchy_consolidated
"""

import os
import sys
import dlt
from pyspark.sql.functions import col, from_json

REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT", 
    os.path.abspath(os.path.join(os.getcwd(), "../../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from claimspandlt.transformations.shared.filestoprocess import FilesToProcess
from claimspandlt.transformations.utils.filehandling import FileHandler
from claimspandlt.transformations.shared.consolidation import ConsolidationProcessor

# Provider Schema Paths
PROVIDER_SCHEMA_PATH = os.path.join(REPO_ROOT, "ClaimsProcessing/dimProvider/Bronze/Schema/provider_schema.json")
PROVIDER_SCHEMA = FileHandler.load_struct_type(PROVIDER_SCHEMA_PATH)

PROVIDER_HIERARCHY_SCHEMA_PATH = os.path.join(REPO_ROOT, "ClaimsProcessing/dimProvider/Bronze/Schema/provider_hierarchy_schema.json")
PROVIDER_HIERARCHY_SCHEMA = FileHandler.load_struct_type(PROVIDER_HIERARCHY_SCHEMA_PATH)

# Consolidation Schema Directories
PROVIDER_CONSOLIDATION_SCHEMA_DIR = os.path.join(REPO_ROOT, "ClaimsProcessing/dimProvider/Bronze/Schema/Consolidation")


@dlt.table(name="bronze_provider_processed")
def bronze_provider_processed():
    """Extracts structured 837 provider records applying schema validation."""
    parsed_df = (
        dlt.read_stream("edi_parsed_mapped")
        .filter((col("status") == "SUCCESS") & (col("extracted_layout_id") == "837"))
        .select(
            from_json(col("edi_parsed"), PROVIDER_SCHEMA).alias("data"),
            col("source_file_path").alias("FILE_ID"),
            col("extracted_client_id").alias("CLIENT_ID"),
            col("extracted_layout_id").alias("FILE_LAYOUT_ID")
        )
        .select("data.*", "FILE_ID", "CLIENT_ID", "FILE_LAYOUT_ID")
    )
    
    # Drop filler columns and add metadata
    from pyspark.sql.functions import lit, current_timestamp, concat
    non_filler_cols = [c for c in parsed_df.columns if not c.startswith("Filler_")]
    return (
        parsed_df.select(non_filler_cols)
        .withColumn("FILE_LAYOUT_DESCRIPTION", concat(lit("Standard"), col("FILE_LAYOUT_ID")))
        .withColumn("LOAD_DATETIME", current_timestamp())
    )


@dlt.table(name="provider_consolidated")
def provider_consolidated():
    """Applies bronze consolidation transformation rules for provider data."""
    stream_df = dlt.read_stream("bronze_provider_processed")
    
    return ConsolidationProcessor.process_consolidation_stream(
        spark=spark,
        df_stream=stream_df,
        ConsolidatedLayerDataModelFilePath=f"{PROVIDER_CONSOLIDATION_SCHEMA_DIR}/DataModels",
        ConsolidatedLayerDataModel="providerdatamodel.json",
        ConsolidatedMappingFilePath=PROVIDER_CONSOLIDATION_SCHEMA_DIR,
        ConsolidatedMappingFileName="ConsolidationProvider.json"
    )


@dlt.table(name="bronze_provider_hierarchy_processed")
def bronze_provider_hierarchy_processed():
    """Extracts structured 274 provider hierarchy records applying schema validation."""
    parsed_df = (
        dlt.read_stream("edi_parsed_mapped")
        .filter((col("status") == "SUCCESS") & (col("extracted_layout_id") == "274"))
        .select(
            from_json(col("edi_parsed"), PROVIDER_HIERARCHY_SCHEMA).alias("data"),
            col("source_file_path").alias("FILE_ID"),
            col("extracted_client_id").alias("CLIENT_ID"),
            col("extracted_layout_id").alias("FILE_LAYOUT_ID")
        )
        .select("data.*", "FILE_ID", "CLIENT_ID", "FILE_LAYOUT_ID")
    )
    
    # Drop filler columns and add metadata
    from pyspark.sql.functions import lit, current_timestamp, concat
    non_filler_cols = [c for c in parsed_df.columns if not c.startswith("Filler_")]
    return (
        parsed_df.select(non_filler_cols)
        .withColumn("FILE_LAYOUT_DESCRIPTION", concat(lit("Standard"), col("FILE_LAYOUT_ID")))
        .withColumn("LOAD_DATETIME", current_timestamp())
    )


@dlt.table(name="provider_hierarchy_consolidated")
def provider_hierarchy_consolidated():
    """Applies bronze consolidation transformation rules for provider hierarchy data."""
    stream_df = dlt.read_stream("bronze_provider_hierarchy_processed")
    
    return ConsolidationProcessor.process_consolidation_stream(
        spark=spark,
        df_stream=stream_df,
        ConsolidatedLayerDataModelFilePath=f"{PROVIDER_CONSOLIDATION_SCHEMA_DIR}/DataModels",
        ConsolidatedLayerDataModel="providerhierarchydatamodel.json",
        ConsolidatedMappingFilePath=PROVIDER_CONSOLIDATION_SCHEMA_DIR,
        ConsolidatedMappingFileName="ConsolidationProviderHierarchy.json"
    )
