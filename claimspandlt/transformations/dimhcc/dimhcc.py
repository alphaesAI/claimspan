"""
dimHCC Pipeline - HCC Dimension and ICD-HCC Crosswalk Processing.

SDP conversion of ClaimsProcessing/dimHCC.
No dependency on ClaimsProcessing/ — data sourced from the CMS ICD-10-CM
mappings CSV pre-staged at /Volumes/claimspan/source/hcc/.

Pipeline datasets:
    Materialized views:
      - bronze_cms_hcc               (raw CMS CSV read, skips 3 metadata lines)
      - silver_hcc_xref_unpivoted   (unpivoted ICD-HCC crosswalk, 5 model versions)
      - silver_hcc_categories        (distinct HCC categories from CMS data)
      - temp_transformed_gold_dimhcc (project silver_hcc with hccKey hash)

    Streaming tables (Auto CDC SCD Type 1 upsert):
      - silver_hcc
      - gold_dimhcc
      - gold_icdhccxref

Note: The config/ and sql/ subdirectories contain the original config-driven
approach for operational data (shcc.json, gdimhcc.json, gicdhccxref.json).
When operational data parquet files become available, those configs can be
used as an alternative source. The CMS CSV is the primary source.
"""

import os
import sys

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, concat_ws, hash as spark_hash

REPO_ROOT = os.environ.get(
    "CLAIMSPAN_REPO_ROOT",
    os.path.abspath(os.path.join(os.getcwd(), "../../.."))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

spark = SparkSession.getActiveSession()

# CMS CSV source path (pre-staged in UC volume — no ClaimsProcessing dependency)
_cms_csv_path = "/Volumes/claimspan/source/hcc/2027 Initial ICD-10-CM Mappings.csv"


# ─── Bronze: Read CMS CSV ──────────────────────────────────────────
@dp.materialized_view(
    name="bronze_cms_hcc",
    comment="Raw CMS HCC ICD-10-CM mappings CSV (skips 3 metadata header lines)"
)
def bronze_cms_hcc(csv_path=_cms_csv_path):
    pdf = pd.read_csv(csv_path, skiprows=3, dtype=str)
    pdf.columns = [c.replace("\n", " ").strip() for c in pdf.columns]
    pdf = pdf.rename(columns={
        pdf.columns[0]: "ICD10",
        pdf.columns[1]: "Description",
        pdf.columns[2]: "V21_ESRD",
        pdf.columns[3]: "V24_ESRD",
        pdf.columns[4]: "V22_COMM",
        pdf.columns[5]: "V28_COMM",
        pdf.columns[6]: "V08_RX"
    })
    # Select only the 7 renamed columns; drop payment-year columns (invalid Delta names)
    pdf = pdf[["ICD10", "Description", "V21_ESRD", "V24_ESRD",
               "V22_COMM", "V28_COMM", "V08_RX"]]
    return spark.createDataFrame(pdf)


# ─── Silver: Unpivot ICD-HCC crosswalk ─────────────────────────────
_cols_xref = ["icd", "icdCodeType", "icdEffectiveYear", "hccNumber",
             "hccVersion", "hccType", "hccEffectiveYear", "isPrimary",
             "effectiveStartDate", "effectiveEndDate"]


def _build_version_df(df, cc_col, version_name, type_name):
    return (
        df.filter(col(cc_col).isNotNull() & (col(cc_col) != "") & (col(cc_col) != "--"))
        .withColumn("icd", col("ICD10"))
        .withColumn("hccNumber", col(cc_col).cast("float").cast("int").cast("string"))
        .withColumn("hccVersion", lit(version_name))
        .withColumn("hccType", lit(type_name))
        .withColumn("icdCodeType", lit("10"))
        .withColumn("icdEffectiveYear", lit(2026))
        .withColumn("hccEffectiveYear", lit(2026))
        .withColumn("isPrimary", lit(True))
        .withColumn("effectiveStartDate", expr("to_date('2026-01-01')"))
        .withColumn("effectiveEndDate", expr("to_date('2026-12-31')"))
    )


@dp.materialized_view(
    name="silver_hcc_xref_unpivoted",
    comment="Unpivoted ICD-HCC crosswalk from CMS CSV (5 model versions: V21, V24, V22, V28, V08)"
)
def silver_hcc_xref_unpivoted():
    df = spark.read.table("bronze_cms_hcc")

    df_v21 = _build_version_df(df, "V21_ESRD", "V21", "ESRD")
    df_v24 = _build_version_df(df, "V24_ESRD", "V24", "ESRD")
    df_v22 = _build_version_df(df, "V22_COMM", "V22", "COMM")
    df_v28 = _build_version_df(df, "V28_COMM", "V28", "COMM")
    df_v08 = _build_version_df(df, "V08_RX", "V08", "RX")

    df_xref = (
        df_v21.select(*_cols_xref)
        .union(df_v24.select(*_cols_xref))
        .union(df_v22.select(*_cols_xref))
        .union(df_v28.select(*_cols_xref))
        .union(df_v08.select(*_cols_xref))
    )

    return (
        df_xref.distinct()
        .withColumn(
            "icdHCCKey",
            spark_hash(concat_ws("|", col("icd"), col("icdCodeType"),
                                 col("hccNumber"), col("hccVersion"),
                                 col("hccType"), col("hccEffectiveYear")))
        )
    )


# ─── Silver: Distinct HCC categories ──────────────────────────────
@dp.materialized_view(
    name="silver_hcc_categories",
    comment="Distinct HCC categories extracted from CMS crosswalk data"
)
def silver_hcc_categories():
    df_xref = spark.read.table("silver_hcc_xref_unpivoted")
    return (
        df_xref.select("hccNumber", "hccVersion", "hccType",
                        "hccEffectiveYear", "effectiveStartDate", "effectiveEndDate")
        .distinct()
        .withColumn("HCCDescription",
                    concat_ws(" ", lit("Hierarchical Condition Category"), col("hccNumber")))
        .withColumn("IsChronic", lit(True))
        .withColumnRenamed("hccEffectiveYear", "EffectiveYear")
        .withColumnRenamed("effectiveStartDate", "EffectiveDateStart")
        .withColumnRenamed("effectiveEndDate", "EffectiveDateEnd")
        .withColumnRenamed("hccNumber", "HCCNumber")
        .withColumnRenamed("hccVersion", "HCCVersion")
        .withColumnRenamed("hccType", "HCCType")
        .withColumn("hashKey",
                    spark_hash(concat_ws("|", col("HCCNumber"), col("HCCVersion"),
                                         col("HCCType"), col("EffectiveYear"))))
    )


# ─── Silver HCC streaming table ───────────────────────────────────
dp.create_streaming_table(
    name="silver_hcc",
    comment="Silver HCC dimension table from CMS data"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="silver_hcc",
    source="silver_hcc_categories",
    keys=["HCCNumber", "HCCVersion", "HCCType", "EffectiveYear"],
    stored_as_scd_type=1
)


# ─── Gold dimHCC (project from silver_hcc with hccKey) ─────────────
@dp.materialized_view(
    name="temp_transformed_gold_dimhcc",
    comment="Transformation for gold_dimhcc: project silver_hcc with hccKey hash"
)
def temp_transformed_gold_dimhcc():
    return (
        spark.read.table("silver_hcc")
        .withColumn("hccKey",
                    spark_hash(concat_ws("|", col("HCCNumber"), col("HCCVersion"),
                                         col("HCCType"), col("EffectiveYear"))))
    )


dp.create_streaming_table(
    name="gold_dimhcc",
    comment="Gold HCC dimension table"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="gold_dimhcc",
    source="temp_transformed_gold_dimhcc",
    keys=["HCCNumber", "HCCVersion", "HCCType", "EffectiveYear"],
    stored_as_scd_type=1
)


# ─── Gold ICD-HCC Xref streaming table ─────────────────────────────
dp.create_streaming_table(
    name="gold_icdhccxref",
    comment="Gold ICD-HCC crosswalk table from CMS data"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="gold_icdhccxref",
    source="silver_hcc_xref_unpivoted",
    keys=["icd", "icdCodeType", "icdEffectiveYear", "hccNumber",
          "hccVersion", "hccType", "hccEffectiveYear",
          "effectiveStartDate", "effectiveEndDate"],
    stored_as_scd_type=1
)