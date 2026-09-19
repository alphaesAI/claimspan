import dlt
from pyspark.sql.functions import col, hash, concat, coalesce, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Explicit schema to avoid CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE error when the volume is empty
alert_group_schema = StructType([
    StructField("AlertGroupID", IntegerType(), False),
    StructField("AlertGroupCode", StringType(), False),
    StructField("AlertGroupDescription", StringType(), False),
    StructField("DisplayText", StringType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("Active", BooleanType(), False)
])

@dlt.table(
    name="silver_alertgroup",
    comment="Cleaned and hashed silver layer for alert groups"
)
@dlt.expect_or_drop("valid_id", "alertGroupKey IS NOT NULL")
def silver_alertgroup():
    source_path = "/Volumes/claimspan/source/alertgroup"
    
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(alert_group_schema)
        .load(source_path)
    )
    
    return df.select(
        col("AlertGroupID").cast("int").alias("alertGroupKey"), # Aliased back to alertGroupKey to match Gold expectations
        col("AlertGroupCode").alias("alertGroupCode"),
        col("AlertGroupDescription").alias("alertGroupDescription"),
        col("DisplayText").alias("displayText"),
        col("SortOrder").cast("int").alias("sortOrder"),
        col("Active").cast("boolean").alias("isActive"),
        hash(
            concat(
                coalesce(col("AlertGroupID").cast("string"), lit("")), lit("|"),
                coalesce(col("AlertGroupCode"), lit("")), lit("|"),
                coalesce(col("AlertGroupDescription"), lit("")), lit("|"),
                coalesce(col("DisplayText"), lit("")), lit("|"),
                coalesce(col("SortOrder").cast("string"), lit("")), lit("|"),
                coalesce(col("Active").cast("string"), lit("false"))
            )
        ).alias("hashKey")
    )

dlt.create_streaming_table(
    name="gold_dimalertgroup",
    comment="Gold dimension table for alert groups"
)

dlt.apply_changes(
    target="gold_dimalertgroup",
    source="silver_alertgroup",
    keys=["alertGroupKey"],
    sequence_by="hashKey",
    stored_as_scd_type=1
)