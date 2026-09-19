import dlt
from pyspark.sql.functions import col, concat_ws, hash, lit, coalesce, current_timestamp

VOLUME_BASE_PATH = "/Volumes/claimspan/source/Client"
CLIENT_CSV_PATH = f"{VOLUME_BASE_PATH}/client_metadata.csv"

@dlt.table(
    name="client_raw",
    comment="Raw client metadata ingested from CSV",
    table_properties={"quality": "bronze"}
)
def client_raw():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(CLIENT_CSV_PATH)

@dlt.table(
    name="temp_client_updates",
    comment="Transformed and deduplicated client updates",
    table_properties={"quality": "silver"}
)
def temp_client_updates():
    df = dlt.read("client_raw")
    return df.select(
        hash(concat_ws("|", col("clientCode"), col("subClientCode"))).cast("bigint").alias("clientKey"),
        col("clientCode"),
        coalesce(col("clientName"), lit("Unspecified")).alias("clientName"),
        col("subClientCode"),
        coalesce(col("subClientName"), lit("Unspecified")).alias("subClientName"),
        current_timestamp().alias("_sequence_num")
    ).dropDuplicates(["clientKey"])

dlt.create_streaming_table(
    name="gold_dimclient",
    comment="Dimension table for clients with automated merge handling",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="gold_dimclient",
    source="temp_client_updates",
    keys=["clientKey"],
    sequence_by=col("_sequence_num"),
    stored_as_scd_type=1
)