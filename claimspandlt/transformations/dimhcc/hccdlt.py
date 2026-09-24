import dlt
from pyspark.sql.functions import col, concat_ws, hash, lit, expr, row_number, coalesce
from pyspark.sql.window import Window

VOLUME_BASE_PATH = "/Volumes/claimspan/source/hcc"
MASTER_CSV_PATH = f"{VOLUME_BASE_PATH}/2027 Initial ICD-10-CM Mappings.csv"

@dlt.table(name="gold_icdhccxref", table_properties={"quality": "gold"})
def gold_icdhccxref():
    raw_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("skipRows", "3") \
        .option("multiLine", "true") \
        .option("pathGlobFilter", "2027 Initial ICD-10-CM Mappings.csv") \
        .load(VOLUME_BASE_PATH)
    for c in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(c, c.replace("\n", " ").strip().replace(" ", "_"))
    cols = raw_df.columns

    def build_version_df(df, cc_col, version_name, type_name):
        return df.filter(col(cc_col).isNotNull() & (col(cc_col) != "") & (col(cc_col) != "--")) \
            .withColumn("icd", col(cols[0])) \
            .withColumn("hccNumber", col(cc_col).cast("float").cast("int").cast("string")) \
            .withColumn("hccVersion", lit(version_name)) \
            .withColumn("hccType", lit(type_name)) \
            .withColumn("icdCodeType", lit("10")) \
            .withColumn("icdEffectiveYear", lit(2026)) \
            .withColumn("hccEffectiveYear", lit(2026)) \
            .withColumn("isPrimary", lit(True)) \
            .withColumn("effectiveStartDate", expr("to_date('2026-01-01')")) \
            .withColumn("effectiveEndDate", expr("to_date('2026-12-31')"))

    df_xref = build_version_df(raw_df, cols[2], "V21", "ESRD") \
        .union(build_version_df(raw_df, cols[3], "V24", "ESRD")) \
        .union(build_version_df(raw_df, cols[4], "V22", "COMM")) \
        .union(build_version_df(raw_df, cols[5], "V28", "COMM")) \
        .union(build_version_df(raw_df, cols[6], "V08", "RX"))

    return df_xref.withColumn("icdHCCKey", hash(concat_ws("|", col("icd"), col("icdCodeType"), col("hccNumber"), col("hccVersion"), col("hccType"), col("hccEffectiveYear"))))

@dlt.table(name="gold_dimhcc", table_properties={"quality": "gold"})
def gold_dimhcc():
    xref = spark.readStream.table("gold_icdhccxref")
    return xref.select("hccNumber", "hccVersion", "hccType", "hccEffectiveYear", "effectiveStartDate", "effectiveEndDate") \
        .distinct() \
        .withColumn("HCCDescription", concat_ws(" ", lit("Hierarchical Condition Category"), col("hccNumber"))) \
        .withColumn("IsChronic", lit(True)) \
        .withColumnRenamed("hccEffectiveYear", "EffectiveYear") \
        .withColumnRenamed("effectiveStartDate", "EffectiveDateStart") \
        .withColumnRenamed("effectiveEndDate", "EffectiveDateEnd") \
        .withColumnRenamed("hccNumber", "HCCNumber") \
        .withColumnRenamed("hccVersion", "HCCVersion") \
        .withColumnRenamed("hccType", "HCCType") \
        .withColumn("hccKey", hash(concat_ws("|", col("HCCNumber"), col("HCCVersion"), col("HCCType"), col("EffectiveYear")))) \
        .withColumn("hashKey", col("hccKey"))