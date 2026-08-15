WITH memAll AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY ESAIInternalPersonID ORDER BY FileID DESC) as rowNum
    FROM claimsprocessing.silver.fhirperson
),

mem AS (
    SELECT 
        -- Master Key & Business Identifiers (FHIR fields)
        ESAIInternalPersonID,
        identifier_enrolleeUniqueID,
        identifier_planMemberID,
        identifier_subscriberID,
        identifier_beneficiaryID,
        
        -- Name fields (FHIR)
        name_family,
        name_given_first,
        name_given_middle,

        birthDate,
        deceasedDateTime,
        gender,
        
        -- Address fields (FHIR)
        address_permanent_line1,
        address_permanent_line2,
        address_permanent_city,
        address_permanent_district,
        address_permanent_state,
        address_permanent_postalCode,
        
        address_mailing_line1,
        address_mailing_line2,
        address_mailing_city,
        address_mailing_state,
        address_mailing_postalCode,
        address_mailing_district,
        
        -- Contact fields (FHIR)
        telecom_phone_home,
        telecom_email,
        telecom_fax,
        
        -- Language fields (FHIR)
        communication_spokenLanguage_text,
        communication_spokenLanguage_codeSystem,
        communication_writtenLanguage_code,
        communication_writtenLanguage_codeSystem,
        communication_otherLanguage_text,
        communication_otherLanguage_codeSystem,

        -- Demographics & Other fields (FHIR)
        extension_race_text,
        extension_race_dataSource,
        extension_ethnicity_ombCategory_code,
        extension_ethnicity_dataSource,
        contact_caretaker_name_given_first,
        contact_caretaker_name_family,
        contact_caretaker_name_given_middle,
        identifier_medicaidID,
        extension_usCitizenStatus AS isUSCitizen,
        extension_maskedMemberID,
        extension_enrolleeEducation,
        extension_enrolleeEmployment,
        extension_coverageProduct_id,

        -- Alternate Keys (FHIR)
        identifier_alternateKey1,
        identifier_alternateKey2,
        identifier_alternateKey3,
        identifier_alternateKey4,
        identifier_alternateKey5,
        identifier_alternateKey6,
        identifier_alternateKey7,
        identifier_alternateKey8,
        identifier_alternateKey9,
        identifier_alternateKey10
    FROM memAll
    WHERE rowNum = 1
)

SELECT 
    CAST(HASH(
        IFNULL(ESAIInternalPersonID, ""), "|",
        IFNULL(identifier_enrolleeUniqueID, ""), "|",
        IFNULL(identifier_planMemberID, ""), "|",
        IFNULL(identifier_subscriberID, ""), "|",
        IFNULL(identifier_beneficiaryID, ""), "|",
        IFNULL(name_family, ""), "|",
        IFNULL(name_given_first, ""), "|",
        IFNULL(name_given_middle, ""), "|",
        IFNULL(CAST(birthDate AS STRING), ""), "|",
        IFNULL(CAST(deceasedDateTime AS STRING), ""), "|",
        IFNULL(gender, ""), "|",
        IFNULL(address_permanent_line1, ""), "|",
        IFNULL(address_permanent_line2, ""), "|",
        IFNULL(address_permanent_city, ""), "|",
        IFNULL(address_permanent_district, ""), "|",
        IFNULL(address_permanent_state, ""), "|",
        IFNULL(address_permanent_postalCode, ""), "|",
        IFNULL(address_mailing_line1, ""), "|",
        IFNULL(address_mailing_line2, ""), "|",
        IFNULL(address_mailing_city, ""), "|",
        IFNULL(address_mailing_state, ""), "|",
        IFNULL(address_mailing_postalCode, ""), "|",
        IFNULL(address_mailing_district, ""), "|",
        IFNULL(telecom_phone_home, ""), "|",
        IFNULL(telecom_email, ""), "|",
        IFNULL(identifier_medicaidID, ""), "|",
        IFNULL(telecom_fax, ""), "|",
        IFNULL(extension_race_text, ""), "|",
        IFNULL(extension_race_dataSource, ""), "|",
        IFNULL(contact_caretaker_name_given_first, ""), "|",
        IFNULL(contact_caretaker_name_family, ""), "|",
        IFNULL(contact_caretaker_name_given_middle, ""), "|",
        IFNULL(extension_ethnicity_ombCategory_code, ""), "|",
        IFNULL(extension_ethnicity_dataSource, ""), "|",
        IFNULL(communication_spokenLanguage_text, ""), "|",
        IFNULL(communication_spokenLanguage_codeSystem, ""), "|",
        IFNULL(isUSCitizen, ""), "|",
        IFNULL(identifier_alternateKey1, ""), "|",
        IFNULL(identifier_alternateKey2, ""), "|",
        IFNULL(identifier_alternateKey3, ""), "|",
        IFNULL(identifier_alternateKey4, ""), "|",
        IFNULL(identifier_alternateKey5, ""), "|",
        IFNULL(identifier_alternateKey6, ""), "|",
        IFNULL(identifier_alternateKey7, ""), "|",
        IFNULL(identifier_alternateKey8, ""), "|",
        IFNULL(identifier_alternateKey9, ""), "|",
        IFNULL(identifier_alternateKey10, ""), "|",
        IFNULL(extension_maskedMemberID, ""), "|",
        IFNULL(extension_enrolleeEducation, ""), "|",
        IFNULL(extension_enrolleeEmployment, ""), "|",
        IFNULL(extension_coverageProduct_id, "")
    ) AS BIGINT) AS memberKey,
    *,
    CURRENT_DATE() AS effectiveStartDate,
    CAST(NULL AS DATE) AS effectiveEndDate,
    TRUE AS isCurrent
FROM mem;