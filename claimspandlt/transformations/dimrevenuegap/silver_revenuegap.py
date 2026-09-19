import dlt
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ─── Source table schemas (placeholder — connect real sources when available) ─
# These temporary views define the expected schema for upstream source tables.
# Replace with real ingestion (Auto Loader, UC tables, etc.) when data is staged.

@dlt.temporary_view()
def BasePrintedDiags():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
        StructField("AlertCategory", StringType(), True),
        StructField("AlertResponseType", IntegerType(), True),
    ])
    return spark.createDataFrame([], schema)


@dlt.temporary_view()
def HCCRaps():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
    ])
    return spark.createDataFrame([], schema)


@dlt.temporary_view()
def MAO004DetailDiagnosis():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
    ])
    return spark.createDataFrame([], schema)


@dlt.table(
    name="silver_revenue_gap",
    comment="Compares suspected chronic condition gaps against RAPS and MAO-004 claims to determine open/closed status."
)
@dlt.expect_or_drop("valid_member_id", "memberIdentifier IS NOT NULL")
def silver_revenue_gap():
    a = spark.read.table("BasePrintedDiags")
    r = spark.read.table("HCCRaps")
    m = spark.read.table("MAO004DetailDiagnosis")

    return (
        a.alias("a")
        .join(r.alias("r"), (col("a.MemberID") == col("r.MemberID")) & (col("a.HCC") == col("r.HCC")), "left")
        .join(m.alias("m"), (col("a.MemberID") == col("m.MemberID")) & (col("a.HCC") == col("m.HCC")), "left")
        .select(
            col("a.MemberID").alias("memberIdentifier"),
            col("a.HCC").alias("hccCode"),
            lit("202601").alias("reportPeriod"),
            lit("new").alias("clientCode"),
            lit(None).cast("string").alias("nameGivenFirst"),
            lit(None).cast("string").alias("nameFamily"),
            lit(None).cast("date").alias("birthDate"),
            lit("V24").alias("hccVersion"),
            lit(None).cast("string").alias("hccDescription"),
            lit("12345").alias("practitionerIdentifier"),
            lit("1234567890").alias("identifierNpi"),
            lit(None).cast("string").alias("practitionerNameFamily"),
            lit(None).cast("string").alias("practitionerNameGivenFirst"),
            lit(None).cast("string").alias("practiceCode"),
            lit(None).cast("string").alias("locationPracticeName"),
            lit(None).cast("string").alias("marketCode"),
            col("a.AlertCategory").alias("alertCategory"),
            when(col("r.MemberID").isNotNull(), lit("Closed by RAPS Claim"))
                .when(col("m.MemberID").isNotNull(), lit("Closed by MAO-004 Encounter"))
                .when(col("a.AlertResponseType") == 1, lit("Closed by Provider Confirmation"))
                .when(col("a.AlertResponseType") == 2, lit("Closed by Provider Rejection"))
                .otherwise(lit(None)).alias("closureReason"),
            lit(None).cast("date").alias("lastDCConfirmedDate"),
            lit(None).cast("date").alias("lastPCPVisitDate"),
            lit(None).cast("date").alias("lastAWVDate"),
            current_date().alias("snapshotDate"),
            lit("PLAN001").alias("payerIdentifier"),
            lit(None).cast("string").alias("revenueGapKey")
        )
    )