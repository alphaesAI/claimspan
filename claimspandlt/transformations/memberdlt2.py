import json
import os
import sys
import dlt
from pyspark.sql.functions import col, udf, expr, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Dynamically pull path from environment variable set in YAML, fallback to current directory
REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT", 
    os.path.abspath(os.path.join(os.getcwd(), "../.."))
)

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Import helper modules
from src.dimember.silver.membergrouping import ProcessMemberBridge
from src.dimember.silver.member import process_silver_member

CONFIG_PATH = os.path.join(REPO_ROOT, "src/dimember/gold/config/dimmember.json")

with open(CONFIG_PATH, "r") as f:
    config_data = json.load(f)


# Step 6: Silver Member Person Bridge Table
@dlt.table(
    name="silver_memberpersonbridge",
    comment="Silver Member Person Bridge Table processed via DLT"
)
def silver_memberpersonbridge():
    df_consolidated = spark.read.table("claimspan.bronzedlt.member_consolidated")
    return ProcessMemberBridge(df_consolidated)


# Step 7: Final Silver Member Table (Materialized View)
@dlt.table(
    name="silver_member",
    comment="Silver Member table"
)
def silver_member():
    df_consolidated = spark.read.table("claimspan.bronzedlt.member_consolidated")
    df_person_bridge = dlt.read("silver_memberpersonbridge")
    
    return process_silver_member(
        df_consolidated=df_consolidated,
        df_person_bridge=df_person_bridge
    )


# Step 8: Dynamic DLT Gold (SCD Type 2)
for entity in config_data.get("SubLayerProcessing", []):
    dest_table = entity["DestinationTable"]
    keys = entity["Keys"]
    sequence_by_col = entity["SequenceBy"]

    dlt.create_streaming_table(
        name=dest_table,
        comment=f"Gold SCD Type 2 table for {entity.get('SubGroupEntity')}"
    )

    dlt.apply_changes(
        target=dest_table,
        source="silver_member",
        keys=keys,
        sequence_by=sequence_by_col,
        stored_as_scd_type="2"
    )