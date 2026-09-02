"""
Silver and Gold Layer DLT Pipeline for Member Domain.

Args:
    None (Reads from upstream DLT table 'member_consolidated').

Returns:
    Target DLT Tables: 'silver_memberpersonbridge', 'silver_member', and Gold SCD Type 2 tables.
"""

import json
import os
import sys
import dlt

REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT", 
    os.path.abspath(os.path.join(os.getcwd(), "../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dimmember.silver.membergrouping import ProcessMemberBridge
from src.dimmember.silver.member import process_silver_member

CONFIG_PATH = os.path.join(REPO_ROOT, "src/dimmember/gold/config/dimmember.json")


def load_gold_config():
    """
    Safely loads Gold layer configuration JSON.

    Returns:
        dict: Parsed JSON content containing sub-layer processing definitions.
    """
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {"SubLayerProcessing": []}


@dlt.table(
    name="silver_memberpersonbridge",
    comment="Silver Member Person Bridge Table processed via DLT"
)
def silver_memberpersonbridge():
    """Creates bridge table between person entity and member dimension."""
    df_consolidated = spark.read.table("member_consolidated")
    return ProcessMemberBridge(df_consolidated)


@dlt.table(
    name="silver_member",
    comment="Silver Member materialized table"
)
def silver_member():
    """Processes silver member dataset combining bridge context."""
    df_consolidated = spark.read.table("member_consolidated")
    df_person_bridge = spark.read.table("silver_memberpersonbridge")
    
    return process_silver_member(
        df_consolidated=df_consolidated,
        df_person_bridge=df_person_bridge
    )


# --- GOLD LAYER SCD TYPE 2 DEFINITIONS ---
config_data = load_gold_config()

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