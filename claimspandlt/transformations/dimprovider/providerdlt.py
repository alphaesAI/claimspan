import dlt
from pyspark.sql.functions import col, lit, current_date, expr, row_number, abs as spark_abs, hash as spark_hash
from pyspark.sql.window import Window

@dlt.table(
    name="gold_dimprovider",
    comment="Gold dimension provider table capturing provider hierarchies and handling Type 2 updates."
)
@dlt.expect_or_drop("valid_provider_id", "practitionerIdentifier IS NOT NULL")
def gold_dimprovider():
    # 1. Read both silver source tables
    pgr = dlt.read("silver_provider_hierarchy")
    prov = dlt.read("silver_provider")

    # 2. Deduplicate hierarchy by latest LoadDateTime per ProviderID
    window_hier = Window.partitionBy(col("ProviderID")).orderBy(col("LoadDateTime").desc())
    pgr_deduped = (
        pgr.withColumn("RowNumber", row_number().over(window_hier))
        .filter(col("RowNumber") == 1)
        .drop("RowNumber")
    )

    # 3. Deduplicate provider by latest LoadDateTime per identifier_providerID
    window_prov = Window.partitionBy(col("identifier_providerID")).orderBy(col("LoadDateTime").desc())
    prov_deduped = (
        prov.withColumn("RowNumber", row_number().over(window_prov))
        .filter(col("RowNumber") == 1)
        .drop("RowNumber")
        .select(
            col("identifier_providerID"),
            col("FirstName"),
            col("MiddleInitial"),
            col("TaxonomyCode1"), col("TaxonomyCode2"), col("TaxonomyCode3"),
            col("TaxonomyCode4"), col("TaxonomyCode5"),
            col("HpSpecialtyCode1"), col("HpSpecialtyCode2"), col("HpSpecialtyCode3"),
            col("HpSpecialtyCode4"), col("HpSpecialtyCode5"),
            col("ADVProviderSpecialtyCode1"), col("ADVProviderSpecialtyCode2"),
            col("ADVProviderSpecialtyCode3"), col("ADVProviderSpecialtyCode4"),
            col("ADVProviderSpecialtyCode5"),
            col("PrescribePrivilege"),
            col("PayorID"),
            col("Contracted"),
            col("ProviderHAI"),
            col("HospitalID"),
            col("ExcludeFromProviderReporting"),
            col("AltProvReporting1"), col("AltProvReporting2"), col("AltProvReporting3"),
            col("AltProvReporting4"), col("AltProvReporting5"), col("AltProvReporting6"),
            col("AltProvReporting7"), col("AltProvReporting8"), col("AltProvReporting9"),
            col("AltProvReporting10")
        )
    )

    # 4. LEFT JOIN hierarchy (base) with provider (enrichment) on ProviderID = identifier_providerID
    combined = pgr_deduped.join(
        prov_deduped,
        pgr_deduped.ProviderID == prov_deduped.identifier_providerID,
        "left"
    )

    # 5. Generate final Gold fields combining hierarchy + provider enrichment
    final_providers = combined.select(
        spark_abs(spark_hash(col("ProviderID"), current_date())).cast("int").alias("providerKey"),
        col("ProviderID").alias("practitionerIdentifier"),
        current_date().alias("effectiveStartDate"),
        lit(None).cast("date").alias("effectiveEndDate"),
        lit(1).alias("isCurrent"),
        col("ProviderNPI").alias("identifierNpi"),
        lit(None).cast("string").alias("identifierDea"),
        col("ProviderLastName").alias("nameFamily"),
        col("FirstName").alias("nameGivenFirst"),
        col("MiddleInitial").alias("nameGivenMiddle"),
        col("PhoneNumber").alias("telecomPhone"),
        col("LocationAddress1").alias("addressLine1"),
        col("LocationAddress2").alias("addressLine2"),
        col("LocationCity").alias("addressCity"),
        col("LocationState").alias("addressState"),
        col("LocationZip").alias("addressPostalCode"),
        col("LocationID").alias("practiceCode"),
        col("LocationDesc").alias("locationPracticeName"),
        col("LocationTIN").alias("providerOrgCode"),
        col("Tier2Desc").alias("organizationName"),
        col("LocationTIN").alias("identifierTin"),
        col("HospitalID").alias("hospitalIdentifier"),
        expr("CASE WHEN ProviderNPI IS NULL THEN '' ELSE LocationDesc END").alias("providerSpecialtyDescription"),
        col("TaxonomyCode1").alias("taxonomyCode1"),
        col("TaxonomyCode2").alias("taxonomyCode2"),
        col("TaxonomyCode3").alias("taxonomyCode3"),
        col("TaxonomyCode4").alias("taxonomyCode4"),
        col("TaxonomyCode5").alias("taxonomyCode5"),
        col("HpSpecialtyCode1").alias("hpSpecialtyCode1"),
        col("ADVProviderSpecialtyCode1").alias("advProviderSpecialtyCode1"),
        col("HpSpecialtyCode2").alias("hpSpecialtyCode2"),
        col("ADVProviderSpecialtyCode2").alias("advProviderSpecialtyCode2"),
        col("HpSpecialtyCode3").alias("hpSpecialtyCode3"),
        col("ADVProviderSpecialtyCode3").alias("advProviderSpecialtyCode3"),
        col("HpSpecialtyCode4").alias("hpSpecialtyCode4"),
        col("ADVProviderSpecialtyCode4").alias("advProviderSpecialtyCode4"),
        col("HpSpecialtyCode5").alias("hpSpecialtyCode5"),
        col("ADVProviderSpecialtyCode5").alias("advProviderSpecialtyCode5"),
        col("PrescribePrivilege").alias("isPrescribePrivilege"),
        col("PayorID").alias("payerIdentifier"),
        col("Contracted").alias("isContracted"),
        col("ProviderHAI").alias("providerHai"),
        col("ExcludeFromProviderReporting").alias("isExcludedFromProviderReporting"),
        lit('Targeted').alias("programType"),
        lit('New - Targeted').alias("practiceTargetedStatus"),
        lit(None).cast("string").alias("productIdentifier"),
        lit(None).cast("string").alias("providerType"),
        col("AltProvReporting1").alias("altProvReporting1"),
        col("AltProvReporting2").alias("altProvReporting2"),
        col("AltProvReporting3").alias("altProvReporting3"),
        col("AltProvReporting4").alias("altProvReporting4"),
        col("AltProvReporting5").alias("altProvReporting5"),
        col("AltProvReporting6").alias("altProvReporting6"),
        col("AltProvReporting7").alias("altProvReporting7"),
        col("AltProvReporting8").alias("altProvReporting8"),
        col("AltProvReporting9").alias("altProvReporting9"),
        col("AltProvReporting10").alias("altProvReporting10")
    )

    return final_providers