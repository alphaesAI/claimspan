import dlt
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ─── Source table schemas (mock data for development/testing) ─
# These temporary views provide mock data matching the MemberRevenueGaps notebook.
# Replace with real ingestion (Auto Loader, UC tables, etc.) when data is staged.

@dlt.temporary_view()
def BasePrintedDiags():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
        StructField("AlertCategory", StringType(), True),
        StructField("AlertResponseType", IntegerType(), True),
    ])
    # Mock data: AlertResponseType 1=Confirmed, 2=Rejected, 0=No response
    # Using real member IDs from gold_dimmember for fact table join compatibility
    data = [
        ("A7001564738", "18", "HIST", 1),   # Suspected diabetes, provider confirmed, RAPS claim found
        ("A7005564738", "18", "SUSP", 0),   # Suspected diabetes, no claim, open gap
        ("MD777888999", "18", "HIST", 0),   # Suspected diabetes, MAO-004 encounter found
        ("A7002564738", "19", "SUSP", 2),   # Suspected diabetes, provider rejected
    ]
    return spark.createDataFrame(data, schema)


@dlt.temporary_view()
def HCCRaps():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
    ])
    # Mock data: submitted RAPS claims
    data = [
        ("A7001564738", "18"),  # Member A7001564738 has diabetes claim in RAPS
        ("A7005564738", "19"),  # Member A7005564738 has diabetes claim in RAPS
    ]
    return spark.createDataFrame(data, schema)


@dlt.temporary_view()
def MAO004DetailDiagnosis():
    schema = StructType([
        StructField("MemberID", StringType(), True),
        StructField("HCC", StringType(), True),
    ])
    # Mock data: submitted MAO-004 encounters
    data = [
        ("A7001564738", "18"),  # Member A7001564738 has encounter in MAO-004
        ("MD777888999", "18"),  # Member MD777888999 has encounter in MAO-004
    ]
    return spark.createDataFrame(data, schema)


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