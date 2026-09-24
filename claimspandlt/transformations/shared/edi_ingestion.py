"""
EDI Ingestion and Extraction Pipeline.

Ingests raw binary EDI files, extracts structural JSON, and maps payloads using 
the MapperFactory.

Target DLT Tables:
    - stg_edi_files
    - edi_extracted_json
    - edi_parsed_mapped

@dlt.table(name='', comment='')
def function_name():
    return 

in python, every calculations are done by one man.
but in spark, the tasks or calculations shared among the workers. 

master ( this function is needs to process, this the input, this is the output we expect // who responsible to share the tasks. ) - (4) worker

"""

import json
import os
import sys
import dlt
from pyspark.sql.functions import col, udf, expr, get_json_object, coalesce
from pyspark.sql.types import StringType

REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT", 
    os.path.abspath(os.path.join(os.getcwd(), "../../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

@udf(returnType=StringType())
def extract_edi_json(file_path: str) -> str:
    """Parses raw EDI file content into JSON using EDIProcessor."""
    if not file_path:
        return None
    try:
        from claimspandlt.transformations.shared.ediprocessing import EDIProcessor
        processor = EDIProcessor()
        return json.dumps(processor(file_path))
    except Exception as e:
        return json.dumps({"error": str(e)})


@udf(returnType=StringType())
def map_edi_json(raw_json_str: str, layout_id: str) -> str:
    """Routes EDI JSON to domain mapper via layout ID."""
    if not raw_json_str or not layout_id:
        return None
    try:
        from claimspandlt.transformations.maps.factory import MapperFactory
        
        raw_json = json.loads(raw_json_str)
        if "error" in raw_json:
            return None
        
        layout_str = str(layout_id).strip()
        if layout_str == "834":
            domain_type = "member"
        elif layout_str == "837":
            domain_type = "provider"
        elif layout_str == "274":
            domain_type = "provider"
        else:
            raise ValueError(f"Unsupported layout ID: {layout_id}")

        mapper = MapperFactory.get_mapper(domain_type)
        return json.dumps(mapper.map(raw_json))
    except Exception as e:
        return json.dumps({"error": str(e)})


@dlt.table(name="stg_edi_files")
def stg_edi_files():
    """Ingests streaming binary EDI source files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile") 
        .load("/Volumes/claimspan/source/all")
        .select(
            col("_metadata.file_path").alias("source_file_path"),
            col("_metadata.file_modification_time").alias("ingested_at")
        )
    )


@dlt.table(name="edi_extracted_json")
def edi_extracted_json():
    """Extracts raw JSON structure and metadata identifiers from EDI files."""
    return (
        dlt.read_stream("stg_edi_files")
        .select(
            col("source_file_path"),
            col("ingested_at"),
            extract_edi_json(col("source_file_path")).alias("raw_extracted_json")
        )
        .withColumn(
            "extracted_client_id", 
            get_json_object(col("raw_extracted_json"), "$.interchange.sender_id")
        )
        .withColumn(
            "extracted_layout_id", 
            coalesce(
                get_json_object(
                    col("raw_extracted_json"), 
                    "$.heading.transaction_set_header_loop.transaction_set_header_ST.transaction_set_identifier_code"
                ),
                get_json_object(
                    col("raw_extracted_json"), 
                    "$.heading.interchange_control_header_loop.interchange_control_header_ST.st01_01"
                )
            )
        )
    )


@dlt.table(name="edi_parsed_mapped")
def edi_parsed_mapped():
    """Maps extracted raw JSON using dynamic factory resolution."""
    return (
        dlt.read_stream("edi_extracted_json")
        .select(
            col("source_file_path"),
            col("ingested_at"),
            col("extracted_client_id"),
            col("extracted_layout_id"),
            map_edi_json(col("raw_extracted_json"), col("extracted_layout_id")).alias("edi_parsed")
        )
        .withColumn(
            "status",
            expr("CASE WHEN edi_parsed = '{}' OR edi_parsed IS NULL OR edi_parsed LIKE '%\"error\"%' THEN 'FAILED' ELSE 'SUCCESS' END")
        )
    )