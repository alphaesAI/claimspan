from pyspark import pipelines as dp
from pyspark.sql import SparkSession
import os
import sys

# Resolve repo root for package imports
REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT",
    os.path.abspath(os.path.join(os.getcwd(), "../../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from claimspandlt.transformations.utils.filehandling import (
    load_json_config,
    resolve_path_placeholders,
    read_source_dataset,
    execute_sql_template,
)

spark = SparkSession.getActiveSession()

# Config and container resolved from repo root — no hardcoding
_client_container = spark.conf.get("spark.claimspan.clientContainer", "claimspan")
_config_file_path = spark.conf.get(
    "spark.claimspan.configPath",
    os.path.join(REPO_ROOT, "claimspandlt", "transformations", "dimclient", "config", "dimclient.json")
)
_base_dir = os.path.dirname(_config_file_path)

# Load configuration dynamically
config_data = load_json_config(_config_file_path)
sub_layer_processing = config_data.get("SubLayerProcessing", [])

# Factory: register a raw materialized view from the configured source
def register_raw_table(ent_name, source_path, source_format):
    @dp.materialized_view(
        name=f"{ent_name}_raw",
        comment=f"Raw ingestion for {ent_name}"
    )
    def _raw(src_path=source_path, src_fmt=source_format):
        return read_source_dataset(spark, src_path, src_fmt)
    return _raw

# Factory: register a transformation materialized view applying SQL to raw data
def register_transform_table(sub_entity, sources, sql_rel_path, base_dir, catalog):
    @dp.materialized_view(
        name=f"temp_transformed_{sub_entity}",
        comment=f"Transformation logic for {sub_entity}"
    )
    def _transform(
        srcs=sources,
        sql_path=sql_rel_path,
        base=base_dir,
        cat=catalog
    ):
        for source_row in srcs:
            ent = source_row.get("Entity")
            spark.read.table(f"{ent}_raw").createOrReplaceTempView(ent)

        return execute_sql_template(
            spark,
            sql_path,
            base,
            cat,
            temp_view_name="tempSQLScript"
        )
    return _transform

# Dynamically iterate and register DLT datasets based on configuration rules
for entity_row in sub_layer_processing:
    sub_group_entity = entity_row.get("SubGroupEntity", "Unknown")
    destination_table = resolve_path_placeholders(
        entity_row.get("DestinationTable", ""), _client_container
    )

    source_tables_config = entity_row.get("SourceTables", [])
    sql_script_rel_path = entity_row.get("SQLScriptPath")

    # 1. Register raw materialized views
    for source_row in source_tables_config:
        entity_name = source_row.get("Entity")
        source_table_path = resolve_path_placeholders(
            source_row.get("SourceTable", ""), _client_container
        )
        source_format = source_row.get("SourceFormat", "delta")
        register_raw_table(entity_name, source_table_path, source_format)

    # 2. Register transformation materialized view
    register_transform_table(
        sub_group_entity,
        source_tables_config,
        sql_script_rel_path,
        _base_dir,
        _client_container
    )

    # 3. Create gold streaming table and apply snapshot CDC (SCD Type 1 upsert)
    dp.create_streaming_table(
        name=destination_table,
        comment=f"Gold production table for {sub_group_entity}"
    )

    dp.create_auto_cdc_from_snapshot_flow(
        target=destination_table,
        source=f"temp_transformed_{sub_group_entity}",
        keys=["clientKey"],
        stored_as_scd_type=1
    )