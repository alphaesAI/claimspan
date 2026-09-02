from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, concat_ws, row_number, sha2, lit, expr, 
    upper, trim, coalesce
)
from pyspark.sql.window import Window


def process_silver_member(
    df_consolidated: DataFrame,
    df_person_bridge: DataFrame,
    spark: SparkSession = None,
    **kwargs
) -> DataFrame:
    """Transforms consolidated member and bridge tables into the Silver Member DataFrame for DLT."""
    ...
    
    # 1. Prepare consolidated member data with UniqueRecord keys
    window_partition = Window.partitionBy(col("FileID")).orderBy(col("RecordHash").desc())

    df_consolidate_mem = (
        df_consolidated.distinct()
        .withColumn(
            "RecordHash",
            sha2(concat_ws("||", *[col(c) for c in df_consolidated.columns]), 256)
        )
        .withColumn("RowNumber", row_number().over(window_partition))
        .withColumn("UniqueRecord", concat_ws("-", col("FileID"), col("RowNumber")))
    )

    # 2. Dynamically supply fallback columns to prevent unresolved column errors
    bridge_cols = set(df_person_bridge.columns)
    df_bridge_prep = df_person_bridge

    if "IsCurrentPMUP" in bridge_cols:
        df_bridge_prep = df_bridge_prep.withColumn("ResolvableIsCurrentPMUP", coalesce(col("IsCurrentPMUP").cast("int"), lit(1)))
    elif "IsCurrent" in bridge_cols:
        df_bridge_prep = df_bridge_prep.withColumn("ResolvableIsCurrentPMUP", coalesce(col("IsCurrent").cast("int"), lit(1)))
    else:
        df_bridge_prep = df_bridge_prep.withColumn("ResolvableIsCurrentPMUP", lit(1))

    if "PMUP" in bridge_cols:
        df_bridge_prep = df_bridge_prep.withColumn("ResolvablePMUP", coalesce(col("PMUP").cast("string"), lit("")))
    else:
        p_member = col("identifier_planMemberID") if "identifier_planMemberID" in bridge_cols else lit("")
        u_key = col("identifier_uniquepersonkey") if "identifier_uniquepersonkey" in bridge_cols else lit("")
        df_bridge_prep = df_bridge_prep.withColumn("ResolvablePMUP", concat_ws("-", p_member, u_key))

    if "ESAIInternalPersonID" not in bridge_cols:
        if "UniqueRecord" in bridge_cols:
            df_bridge_prep = df_bridge_prep.withColumn("ESAIInternalPersonID", col("UniqueRecord"))
        else:
            df_bridge_prep = df_bridge_prep.withColumn("ESAIInternalPersonID", lit(""))

    # 3. Join consolidated member data with bridge prep dataset
    joined_df = df_consolidate_mem.alias("mem").join(
        df_bridge_prep.alias("brdg"),
        (col("mem.UniqueRecord") == col("brdg.UniqueRecord")) &
        (col("mem.FileLayoutID") == col("brdg.FileLayoutID")) &
        (col("brdg.ResolvableIsCurrentPMUP") == 1),
        "inner"
    )

    # 4. Transform FHIR attributes, demographics, and alternate keys
    transformed_df = joined_df.select(
        col("brdg.ESAIInternalPersonID").alias("ESAIInternalPersonID"),
        col("brdg.UniqueRecord").alias("UniqueRecord"),
        col("mem.ClientID").alias("ClientID"),
        col("mem.FileID").alias("FileID"),
        col("mem.LoadDateTime").alias("LoadDateTime"),
        col("mem.FileLayoutID").alias("FileLayoutID"),
        col("mem.FileLayoutDescription").alias("FileLayoutDescription"),
        # FHIR Identifiers
        upper(trim(col("mem.identifier_uniquepersonkey"))).alias("identifier_uniquepersonkey"),
        upper(trim(col("mem.identifier_planMemberID"))).alias("identifier_planMemberID"),
        upper(trim(col("mem.identifier_subscriberID"))).alias("identifier_subscriberID"),
        upper(trim(col("mem.identifier_beneficiaryID"))).alias("identifier_beneficiaryID"),
        upper(trim(col("mem.identifier_enrolleeUniqueID"))).alias("identifier_enrolleeUniqueID"),
        # FHIR Name fields
        upper(trim(col("mem.name_family"))).alias("name_family"),
        upper(trim(col("mem.name_given_first"))).alias("name_given_first"),
        upper(trim(col("mem.name_given_middle"))).alias("name_given_middle"),
        upper(trim(col("mem.name_prefix"))).alias("name_prefix"),
        upper(trim(col("mem.name_suffix"))).alias("name_suffix"),
        col("mem.name_text").alias("name_text"),
        # Dates & Demographics
        expr("try_cast(mem.birthDate as string)").alias("birthDate"),
        expr("try_cast(mem.deceasedDateTime as string)").alias("deceasedDateTime"),
        upper(trim(col("mem.gender"))).alias("gender"),
        # Permanent Address
        upper(trim(col("mem.address_permanent_line1"))).alias("address_permanent_line1"),
        upper(trim(col("mem.address_permanent_line2"))).alias("address_permanent_line2"),
        upper(trim(col("mem.address_permanent_city"))).alias("address_permanent_city"),
        upper(trim(col("mem.address_permanent_district"))).alias("address_permanent_district"),
        upper(trim(col("mem.address_permanent_state"))).alias("address_permanent_state"),
        upper(trim(col("mem.address_permanent_postalCode"))).alias("address_permanent_postalCode"),
        # Mailing Address
        upper(trim(col("mem.address_mailing_line1"))).alias("address_mailing_line1"),
        upper(trim(col("mem.address_mailing_line2"))).alias("address_mailing_line2"),
        upper(trim(col("mem.address_mailing_city"))).alias("address_mailing_city"),
        upper(trim(col("mem.address_mailing_state"))).alias("address_mailing_state"),
        upper(trim(col("mem.address_mailing_postalCode"))).alias("address_mailing_postalCode"),
        upper(trim(col("mem.address_mailing_district"))).alias("address_mailing_district"),
        # Telecom
        trim(col("mem.telecom_phone_home")).alias("telecom_phone_home"),
        trim(col("mem.telecom_email")).alias("telecom_email"),
        trim(col("mem.telecom_fax")).alias("telecom_fax"),
        # Additional Identifiers & Extensions
        upper(trim(col("mem.identifier_medicaidID"))).alias("identifier_medicaidID"),
        upper(trim(col("mem.extension_maskedMemberID"))).alias("extension_maskedMemberID"),
        upper(trim(col("mem.extension_race_text"))).alias("extension_race_text"),
        upper(trim(col("mem.extension_race_dataSource"))).alias("extension_race_dataSource"),
        upper(trim(col("mem.extension_ethnicity_ombCategory_code"))).alias("extension_ethnicity_ombCategory_code"),
        upper(trim(col("mem.extension_ethnicity_dataSource"))).alias("extension_ethnicity_dataSource"),
        # Communication
        upper(trim(col("mem.communication_spokenLanguage_text"))).alias("communication_spokenLanguage_text"),
        upper(trim(col("mem.communication_spokenLanguage_codeSystem"))).alias("communication_spokenLanguage_codeSystem"),
        upper(trim(col("mem.communication_writtenLanguage_code"))).alias("communication_writtenLanguage_code"),
        upper(trim(col("mem.communication_writtenLanguage_codeSystem"))).alias("communication_writtenLanguage_codeSystem"),
        upper(trim(col("mem.communication_otherLanguage_text"))).alias("communication_otherLanguage_text"),
        upper(trim(col("mem.communication_otherLanguage_codeSystem"))).alias("communication_otherLanguage_codeSystem"),
        # Contact Caretaker
        upper(trim(col("mem.contact_caretaker_name_given_first"))).alias("contact_caretaker_name_given_first"),
        upper(trim(col("mem.contact_caretaker_name_family"))).alias("contact_caretaker_name_family"),
        upper(trim(col("mem.contact_caretaker_name_given_middle"))).alias("contact_caretaker_name_given_middle"),
        # Employment & Status
        upper(trim(col("mem.extension_usCitizenStatus"))).alias("extension_usCitizenStatus"),
        upper(trim(col("mem.extension_enrolleeEducation"))).alias("extension_enrolleeEducation"),
        upper(trim(col("mem.extension_enrolleeEmployment"))).alias("extension_enrolleeEmployment"),
        upper(trim(col("mem.extension_coverageProduct_id"))).alias("extension_coverageProduct_id"),
        # Alternate Keys
        col("mem.identifier_alternateKey1").alias("identifier_alternateKey1"),
        col("mem.identifier_alternateKey2").alias("identifier_alternateKey2"),
        col("mem.identifier_alternateKey3").alias("identifier_alternateKey3"),
        col("mem.identifier_alternateKey4").alias("identifier_alternateKey4"),
        col("mem.identifier_alternateKey5").alias("identifier_alternateKey5"),
        col("mem.identifier_alternateKey6").alias("identifier_alternateKey6"),
        col("mem.identifier_alternateKey7").alias("identifier_alternateKey7"),
        col("mem.identifier_alternateKey8").alias("identifier_alternateKey8"),
        col("mem.identifier_alternateKey9").alias("identifier_alternateKey9"),
        col("mem.identifier_alternateKey10").alias("identifier_alternateKey10"),
        # Bridge Metadata
        col("brdg.ResolvablePMUP").alias("PMUP"),
        col("brdg.ResolvableIsCurrentPMUP").alias("IsCurrentPMUP")
    )

    # 5. Build SHA-256 HashKey column and return final structured dataset
    return transformed_df.withColumn(
        "HashKey",
        sha2(concat_ws("|",
            expr("IFNULL(ESAIInternalPersonID, '')"), expr("IFNULL(UniqueRecord, '')"),
            expr("IFNULL(ClientID, '')"), expr("IFNULL(CAST(FileID AS STRING), '')"),
            expr("IFNULL(CAST(LoadDateTime AS STRING), '')"), expr("IFNULL(CAST(FileLayoutID AS STRING), '')"),
            expr("IFNULL(FileLayoutDescription, '')"), expr("IFNULL(identifier_uniquepersonkey, '')"),
            expr("IFNULL(identifier_planMemberID, '')"), expr("IFNULL(identifier_subscriberID, '')"),
            expr("IFNULL(identifier_beneficiaryID, '')"), expr("IFNULL(identifier_enrolleeUniqueID, '')"),
            expr("IFNULL(name_family, '')"), expr("IFNULL(name_given_first, '')"),
            expr("IFNULL(name_given_middle, '')"), expr("IFNULL(CAST(birthDate AS STRING), '')"),
            expr("IFNULL(CAST(deceasedDateTime AS STRING), '')"), expr("IFNULL(gender, '')"),
            expr("IFNULL(address_permanent_line1, '')"), expr("IFNULL(address_permanent_line2, '')"),
            expr("IFNULL(address_permanent_city, '')"), expr("IFNULL(address_permanent_district, '')"),
            expr("IFNULL(address_permanent_state, '')"), expr("IFNULL(address_permanent_postalCode, '')"),
            expr("IFNULL(address_mailing_line1, '')"), expr("IFNULL(address_mailing_line2, '')"),
            expr("IFNULL(address_mailing_city, '')"), expr("IFNULL(address_mailing_state, '')"),
            expr("IFNULL(address_mailing_postalCode, '')"), expr("IFNULL(address_mailing_district, '')"),
            expr("IFNULL(telecom_phone_home, '')"), expr("IFNULL(telecom_email, '')"),
            expr("IFNULL(identifier_medicaidID, '')"), expr("IFNULL(telecom_fax, '')"),
            expr("IFNULL(extension_race_text, '')"), expr("IFNULL(extension_race_dataSource, '')"),
            expr("IFNULL(contact_caretaker_name_given_first, '')"), expr("IFNULL(contact_caretaker_name_family, '')"),
            expr("IFNULL(contact_caretaker_name_given_middle, '')"), expr("IFNULL(extension_ethnicity_ombCategory_code, '')"),
            expr("IFNULL(extension_ethnicity_dataSource, '')"), expr("IFNULL(communication_spokenLanguage_text, '')"),
            expr("IFNULL(communication_spokenLanguage_codeSystem, '')"), expr("IFNULL(communication_writtenLanguage_code, '')"),
            expr("IFNULL(communication_writtenLanguage_codeSystem, '')"), expr("IFNULL(communication_otherLanguage_text, '')"),
            expr("IFNULL(communication_otherLanguage_codeSystem, '')"), expr("IFNULL(extension_usCitizenStatus, '')"),
            expr("IFNULL(identifier_alternateKey1, '')"), expr("IFNULL(identifier_alternateKey2, '')"),
            expr("IFNULL(identifier_alternateKey3, '')"), expr("IFNULL(identifier_alternateKey4, '')"),
            expr("IFNULL(identifier_alternateKey5, '')"), expr("IFNULL(identifier_alternateKey6, '')"),
            expr("IFNULL(identifier_alternateKey7, '')"), expr("IFNULL(identifier_alternateKey8, '')"),
            expr("IFNULL(identifier_alternateKey9, '')"), expr("IFNULL(identifier_alternateKey10, '')"),
            expr("IFNULL(extension_maskedMemberID, '')"), expr("IFNULL(extension_enrolleeEducation, '')"),
            expr("IFNULL(extension_enrolleeEmployment, '')"), expr("IFNULL(PMUP, '')"),
            expr("IFNULL(CAST(IsCurrentPMUP AS STRING), '')"), expr("IFNULL(extension_coverageProduct_id, '')"),
            expr("IFNULL(name_prefix, '')"), expr("IFNULL(name_suffix, '')"),
            expr("IFNULL(name_text, '')")
        ), 256)
    ).select(
        "ESAIInternalPersonID", "UniqueRecord", "ClientID", "FileID", "LoadDateTime", "FileLayoutID",
        "FileLayoutDescription", "identifier_uniquepersonkey", "identifier_planMemberID", "identifier_subscriberID",
        "identifier_beneficiaryID", "identifier_enrolleeUniqueID", "name_family", "name_given_first",
        "name_given_middle", "birthDate", "deceasedDateTime", "gender", "address_permanent_line1",
        "address_permanent_line2", "address_permanent_city", "address_permanent_district",
        "address_permanent_state", "address_permanent_postalCode", "address_mailing_line1",
        "address_mailing_line2", "address_mailing_city", "address_mailing_state", "address_mailing_postalCode",
        "address_mailing_district", "telecom_phone_home", "telecom_email", "identifier_medicaidID",
        "telecom_fax", "extension_race_text", "extension_race_dataSource", "contact_caretaker_name_given_first",
        "contact_caretaker_name_family", "contact_caretaker_name_given_middle", "extension_ethnicity_ombCategory_code",
        "extension_ethnicity_dataSource", "communication_spokenLanguage_text", "communication_spokenLanguage_codeSystem",
        "communication_writtenLanguage_code", "communication_writtenLanguage_codeSystem",
        "communication_otherLanguage_text", "communication_otherLanguage_codeSystem", "extension_usCitizenStatus",
        "identifier_alternateKey1", "identifier_alternateKey2", "identifier_alternateKey3", "identifier_alternateKey4",
        "identifier_alternateKey5", "identifier_alternateKey6", "identifier_alternateKey7", "identifier_alternateKey8",
        "identifier_alternateKey9", "identifier_alternateKey10", "extension_maskedMemberID", "extension_enrolleeEducation",
        "extension_enrolleeEmployment", "PMUP", "IsCurrentPMUP", "extension_coverageProduct_id", "name_prefix",
        "name_suffix", "name_text", "HashKey"
    )