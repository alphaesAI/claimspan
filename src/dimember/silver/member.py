from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, row_number, sha2, lit, expr
from pyspark.sql.window import Window

# SQL definition for silver member transformation
SILVER_MEMBER_SQL = """
WITH silverPersonBrdgPrep AS (
  SELECT 
    *,
    CAST(COALESCE(IsCurrentPMUP, 1) AS INT) AS ResolvableIsCurrentPMUP,
    CAST(COALESCE(PMUP, '') AS STRING) AS ResolvablePMUP
  FROM silverPersonBrdg
),
consolidateMem1 AS (
  SELECT 
    brdg.ESAIInternalPersonID, 
    brdg.UniqueRecord, 
    mem.ClientID, 
    mem.FileID, 
    mem.LoadDateTime, 
    mem.FileLayoutID, 
    mem.FileLayoutDescription,
    -- FHIR Identifiers
    UPPER(TRIM(mem.identifier_uniquepersonkey)) as identifier_uniquepersonkey,
    UPPER(TRIM(mem.identifier_planMemberID)) as identifier_planMemberID,
    UPPER(TRIM(mem.identifier_subscriberID)) as identifier_subscriberID,
    UPPER(TRIM(mem.identifier_beneficiaryID)) as identifier_beneficiaryID,
    UPPER(TRIM(mem.identifier_enrolleeUniqueID)) as identifier_enrolleeUniqueID,
    -- FHIR Name fields
    UPPER(TRIM(mem.name_family)) as name_family,
    UPPER(TRIM(mem.name_given_first)) as name_given_first,
    UPPER(TRIM(mem.name_given_middle)) as name_given_middle,
    UPPER(TRIM(mem.name_prefix)) as name_prefix,
    UPPER(TRIM(mem.name_suffix)) as name_suffix,
    mem.name_text as name_text,
    -- Dates & Demographics
    TRY_CAST(mem.birthDate AS STRING) as birthDate,
    TRY_CAST(mem.deceasedDateTime AS STRING) as deceasedDateTime,
    UPPER(TRIM(mem.gender)) as gender,
    -- Permanent Address
    UPPER(TRIM(mem.address_permanent_line1)) as address_permanent_line1,
    UPPER(TRIM(mem.address_permanent_line2)) as address_permanent_line2,
    UPPER(TRIM(mem.address_permanent_city)) as address_permanent_city,
    UPPER(TRIM(mem.address_permanent_district)) as address_permanent_district,
    UPPER(TRIM(mem.address_permanent_state)) as address_permanent_state,
    UPPER(TRIM(mem.address_permanent_postalCode)) as address_permanent_postalCode,
    -- Mailing Address
    UPPER(TRIM(mem.address_mailing_line1)) as address_mailing_line1,
    UPPER(TRIM(mem.address_mailing_line2)) as address_mailing_line2,
    UPPER(TRIM(mem.address_mailing_city)) as address_mailing_city,
    UPPER(TRIM(mem.address_mailing_state)) as address_mailing_state,
    UPPER(TRIM(mem.address_mailing_postalCode)) as address_mailing_postalCode,
    UPPER(TRIM(mem.address_mailing_district)) as address_mailing_district,
    -- Telecom
    TRIM(mem.telecom_phone_home) as telecom_phone_home,
    TRIM(mem.telecom_email) as telecom_email,
    TRIM(mem.telecom_fax) as telecom_fax,
    -- Additional Identifiers & Extensions
    UPPER(TRIM(mem.identifier_medicaidID)) as identifier_medicaidID,
    UPPER(TRIM(mem.extension_maskedMemberID)) as extension_maskedMemberID,
    UPPER(TRIM(mem.extension_race_text)) as extension_race_text,
    UPPER(TRIM(mem.extension_race_dataSource)) as extension_race_dataSource,
    UPPER(TRIM(mem.extension_ethnicity_ombCategory_code)) as extension_ethnicity_ombCategory_code,
    UPPER(TRIM(mem.extension_ethnicity_dataSource)) as extension_ethnicity_dataSource,
    -- Communication
    UPPER(TRIM(mem.communication_spokenLanguage_text)) as communication_spokenLanguage_text,
    UPPER(TRIM(mem.communication_spokenLanguage_codeSystem)) as communication_spokenLanguage_codeSystem,
    UPPER(TRIM(mem.communication_writtenLanguage_code)) as communication_writtenLanguage_code,
    UPPER(TRIM(mem.communication_writtenLanguage_codeSystem)) as communication_writtenLanguage_codeSystem,
    UPPER(TRIM(mem.communication_otherLanguage_text)) as communication_otherLanguage_text,
    UPPER(TRIM(mem.communication_otherLanguage_codeSystem)) as communication_otherLanguage_codeSystem,
    -- Contact Caretaker
    UPPER(TRIM(mem.contact_caretaker_name_given_first)) as contact_caretaker_name_given_first,
    UPPER(TRIM(mem.contact_caretaker_name_family)) as contact_caretaker_name_family,
    UPPER(TRIM(mem.contact_caretaker_name_given_middle)) as contact_caretaker_name_given_middle,
    -- Employment & Status
    UPPER(TRIM(mem.extension_usCitizenStatus)) as extension_usCitizenStatus,
    UPPER(TRIM(mem.extension_enrolleeEducation)) as extension_enrolleeEducation,
    UPPER(TRIM(mem.extension_enrolleeEmployment)) as extension_enrolleeEmployment,
    UPPER(TRIM(mem.extension_coverageProduct_id)) as extension_coverageProduct_id,
    -- Alternate Keys
    mem.identifier_alternateKey1 as identifier_alternateKey1,
    mem.identifier_alternateKey2 as identifier_alternateKey2,
    mem.identifier_alternateKey3 as identifier_alternateKey3,
    mem.identifier_alternateKey4 as identifier_alternateKey4,
    mem.identifier_alternateKey5 as identifier_alternateKey5,
    mem.identifier_alternateKey6 as identifier_alternateKey6,
    mem.identifier_alternateKey7 as identifier_alternateKey7,
    mem.identifier_alternateKey8 as identifier_alternateKey8,
    mem.identifier_alternateKey9 as identifier_alternateKey9,
    mem.identifier_alternateKey10 as identifier_alternateKey10,
    -- Bridge Metadata
    brdg.ResolvablePMUP AS PMUP, 
    brdg.ResolvableIsCurrentPMUP AS IsCurrentPMUP
  FROM consolidateMem mem
  INNER JOIN silverPersonBrdgPrep brdg 
    ON mem.UniqueRecord = brdg.UniqueRecord 
    AND mem.FileLayoutID = brdg.FileLayoutID 
    AND brdg.ResolvableIsCurrentPMUP = 1
)
SELECT 
  ESAIInternalPersonID, UniqueRecord, ClientID, FileID, LoadDateTime, FileLayoutID, 
  FileLayoutDescription, identifier_uniquepersonkey, identifier_planMemberID, identifier_subscriberID, identifier_beneficiaryID, identifier_enrolleeUniqueID,
  name_family, name_given_first, name_given_middle, birthDate, deceasedDateTime, 
  gender, address_permanent_line1, address_permanent_line2, address_permanent_city, address_permanent_district, 
  address_permanent_state, address_permanent_postalCode, address_mailing_line1, address_mailing_line2, 
  address_mailing_city, address_mailing_state, address_mailing_postalCode, address_mailing_district, telecom_phone_home, telecom_email, 
  identifier_medicaidID, telecom_fax, extension_race_text, extension_race_dataSource, contact_caretaker_name_given_first, contact_caretaker_name_family, 
  contact_caretaker_name_given_middle, extension_ethnicity_ombCategory_code, extension_ethnicity_dataSource, communication_spokenLanguage_text, 
  communication_spokenLanguage_codeSystem, communication_writtenLanguage_code, communication_writtenLanguage_codeSystem, 
  communication_otherLanguage_text, communication_otherLanguage_codeSystem, extension_usCitizenStatus, identifier_alternateKey1, identifier_alternateKey2, 
  identifier_alternateKey3, identifier_alternateKey4, identifier_alternateKey5, identifier_alternateKey6, identifier_alternateKey7, 
  identifier_alternateKey8, identifier_alternateKey9, identifier_alternateKey10, extension_maskedMemberID, extension_enrolleeEducation, 
  extension_enrolleeEmployment, PMUP, IsCurrentPMUP, extension_coverageProduct_id, name_prefix, name_suffix, name_text,
  sha2(concat(
    IfNull(ESAIInternalPersonID,""), "|", IfNull(UniqueRecord,""), "|", 
    IfNull(ClientID,""), "|", IfNull(CAST(FileID AS STRING),""), "|", 
    IfNull(CAST(LoadDateTime AS STRING),""), "|", IfNull(CAST(FileLayoutID AS STRING),""), "|", 
    IfNull(FileLayoutDescription,""), "|", IfNull(identifier_uniquepersonkey,""), "|", 
    IfNull(identifier_planMemberID,""), "|", IfNull(identifier_subscriberID,""), "|", 
    IfNull(identifier_beneficiaryID,""), "|", 
    IfNull(identifier_enrolleeUniqueID, ""), "|",
    IfNull(name_family,""), "|", IfNull(name_given_first,""), "|", 
    IfNull(name_given_middle,""), "|", 
    IfNull(CAST(birthDate AS STRING),""), "|", IfNull(CAST(deceasedDateTime AS STRING),""), "|", 
    IfNull(gender,""), "|", IfNull(address_permanent_line1,""), "|", 
    IfNull(address_permanent_line2,""), "|", IfNull(address_permanent_city,""), "|", 
    IfNull(address_permanent_district,""), "|", IfNull(address_permanent_state,""), "|", 
    IfNull(address_permanent_postalCode,""), "|", IfNull(address_mailing_line1,""), "|", 
    IfNull(address_mailing_line2,""), "|", IfNull(address_mailing_city,""), "|", 
    IfNull(address_mailing_state,""), "|", IfNull(address_mailing_postalCode,""), "|", 
    IfNull(address_mailing_district,""), "|", IfNull(telecom_phone_home,""), "|", IfNull(telecom_email,""), "|", 
    IfNull(identifier_medicaidID,""), "|", IfNull(telecom_fax,""), "|", IfNull(extension_race_text,""), "|", 
    IfNull(extension_race_dataSource,""), "|", IfNull(contact_caretaker_name_given_first,""), "|", 
    IfNull(contact_caretaker_name_family,""), "|", IfNull(contact_caretaker_name_given_middle,""), "|", 
    IfNull(extension_ethnicity_ombCategory_code,""), "|", IfNull(extension_ethnicity_dataSource,""), "|", 
    IfNull(communication_spokenLanguage_text,""), "|", IfNull(communication_spokenLanguage_codeSystem,""), "|", 
    IfNull(communication_writtenLanguage_code,""), "|", IfNull(communication_writtenLanguage_codeSystem,""), "|", 
    IfNull(communication_otherLanguage_text,""), "|", IfNull(communication_otherLanguage_codeSystem,""), "|", 
    IfNull(extension_usCitizenStatus,""), "|", IfNull(identifier_alternateKey1,""), "|", IfNull(identifier_alternateKey2,""), "|", 
    IfNull(identifier_alternateKey3,""), "|", IfNull(identifier_alternateKey4,""), "|", 
    IfNull(identifier_alternateKey5,""), "|", IfNull(identifier_alternateKey6,""), "|", 
    IfNull(identifier_alternateKey7,""), "|", IfNull(identifier_alternateKey8,""), "|", 
    IfNull(identifier_alternateKey9,""), "|", IfNull(identifier_alternateKey10,""), "|", 
    IfNull(extension_maskedMemberID,""), "|", IfNull(extension_enrolleeEducation,""), "|", 
    IfNull(extension_enrolleeEmployment,""), "|", IfNull(PMUP,""), "|", 
    IfNull(CAST(IsCurrentPMUP AS STRING),""), "|", IfNull(extension_coverageProduct_id,""), "|", 
    IfNull(name_prefix,""), "|", IfNull(name_suffix,""), "|", IfNull(name_text,"")
  ), 256) AS HashKey 
FROM consolidateMem1
"""


def process_silver_member(
    spark: SparkSession,
    df_consolidated: DataFrame,
    df_person_bridge: DataFrame,
) -> DataFrame:
    """Transforms consolidated member and bridge tables into the Silver Member DataFrame."""
    window_partition = Window.partitionBy(col("FileID")).orderBy(
        col("RecordHash").desc()
    )

    # 1. Prepare consolidated member data with UniqueRecord keys
    df_consolidate_mem = (
        df_consolidated.distinct()
        .withColumn(
            "RecordHash",
            sha2(
                concat_ws("||", *[col(c) for c in df_consolidated.columns]), 256
            ),
        )
        .withColumn("RowNumber", row_number().over(window_partition))
        .withColumn(
            "UniqueRecord", concat_ws("-", col("FileID"), col("RowNumber"))
        )
    )

    # 2. Dynamically supply fallback columns without throwing UNRESOLVED_COLUMN errors
    df_bridge_guaranteed = df_person_bridge
    bridge_cols = set(df_person_bridge.columns)

    if "IsCurrentPMUP" not in bridge_cols:
        if "IsCurrent" in bridge_cols:
            df_bridge_guaranteed = df_bridge_guaranteed.withColumn("IsCurrentPMUP", col("IsCurrent"))
        else:
            df_bridge_guaranteed = df_bridge_guaranteed.withColumn("IsCurrentPMUP", lit(1))

    if "PMUP" not in bridge_cols:
        p_member = col("identifier_planMemberID") if "identifier_planMemberID" in bridge_cols else lit("")
        u_key = col("identifier_uniquepersonkey") if "identifier_uniquepersonkey" in bridge_cols else lit("")
        df_bridge_guaranteed = df_bridge_guaranteed.withColumn("PMUP", concat_ws("-", p_member, u_key))

    if "ESAIInternalPersonID" not in bridge_cols:
        if "UniqueRecord" in bridge_cols:
            df_bridge_guaranteed = df_bridge_guaranteed.withColumn("ESAIInternalPersonID", col("UniqueRecord"))
        else:
            df_bridge_guaranteed = df_bridge_guaranteed.withColumn("ESAIInternalPersonID", lit(""))

    # 3. Register temporary views for SQL execution
    df_consolidate_mem.createOrReplaceTempView("consolidateMem")
    df_bridge_guaranteed.createOrReplaceTempView("silverPersonBrdg")

    # 4. Execute Spark SQL query
    return spark.sql(SILVER_MEMBER_SQL)