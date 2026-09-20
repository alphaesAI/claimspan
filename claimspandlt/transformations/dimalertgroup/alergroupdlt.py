import dlt
from pyspark.sql.functions import col, concat, lit, coalesce, hash, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

VOLUME_BASE_PATH = "/Volumes/claimspan/source/alertgroup"
CSV_PATH = f"{VOLUME_BASE_PATH}/alert_group_reference.csv"

# Schema matching original CSV definition
schema = StructType([
    StructField("AlertGroupID", IntegerType(), False),
    StructField("AlertGroupCode", StringType(), False),
    StructField("AlertGroupDescription", StringType(), False),
    StructField("DisplayText", StringType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("Active", BooleanType(), False)
])

# ==========================================
# 1. BRONZE / RAW INGESTION (Streaming Source)
# ==========================================
@dlt.table(
    name="raw_alertgroup",
    comment="Raw alert group data stream from CSV",
    table_properties={"quality": "bronze"}
)
def raw_alertgroup():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(VOLUME_BASE_PATH)


# ==========================================
# 2. SILVER LAYER: silver_alertgroup
# ==========================================
@dlt.table(
    name="temp_silver_alertgroup",
    comment="Transformed and hashed alert group records for silver layer",
    table_properties={"quality": "silver"}
)
def temp_silver_alertgroup():
    df = dlt.read_stream("raw_alertgroup")
    return df.select(
        col("AlertGroupID").alias("alertGroupID"),
        col("AlertGroupCode").alias("alertGroupCode"),
        col("AlertGroupDescription").alias("alertGroupDescription"),
        col("DisplayText").alias("displayText"),
        col("SortOrder").alias("sortOrder"),
        col("Active").alias("isActive"),
        hash(
            concat(
                coalesce(col("AlertGroupID").cast("string"), lit("")), lit("|"),
                coalesce(col("AlertGroupCode"), lit("")), lit("|"),
                coalesce(col("AlertGroupDescription"), lit("")), lit("|"),
                coalesce(col("DisplayText"), lit("")), lit("|"),
                coalesce(col("SortOrder").cast("string"), lit("")), lit("|"),
                coalesce(col("Active").cast("string"), lit("false"))
            )
        ).alias("hashKey"),
        current_timestamp().alias("_sequence_num")
    )

dlt.create_streaming_table(
    name="silver_alertgroup",
    comment="Cleaned silver alert group dimension table with SCD Type 1 upserts",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_alertgroup",
    source="temp_silver_alertgroup",
    keys=["alertGroupID"],
    sequence_by=col("_sequence_num"),
    stored_as_scd_type=1
)


# ==========================================
# 3. GOLD LAYER: gold_dimalertgroup
# ==========================================
@dlt.table(
    name="temp_gold_dimalertgroup",
    comment="Prepared gold alert group dimension updates",
    table_properties={"quality": "gold"}
)
def temp_gold_dimalertgroup():
    df = spark.readStream.option("skipChangeCommits", "true").table("silver_alertgroup")
    return df.select(
        col("alertGroupID").alias("alertGroupKey"),
        col("alertGroupCode"),
        col("alertGroupDescription"),
        col("displayText"),
        col("sortOrder"),
        col("isActive"),
        current_timestamp().alias("_sequence_num")
    )

dlt.create_streaming_table(
    name="gold_dimalertgroup",
    comment="Final conformed gold dimension table for alert groups",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="gold_dimalertgroup",
    source="temp_gold_dimalertgroup",
    keys=["alertGroupKey"],
    sequence_by=col("_sequence_num"),
    stored_as_scd_type=1
)