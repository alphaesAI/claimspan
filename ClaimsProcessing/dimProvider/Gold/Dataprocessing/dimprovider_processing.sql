WITH ProviderHierarchy AS (
  SELECT 
     pgr.identifier_providerID                       AS ESAIInternalProviderID
    ,pgr.identifier_npi                              AS identifier_npi
    ,pgr.identifier_tin                              AS identifier_tin
    ,pgr.name_family                                 AS name_family
    ,CAST(NULL AS string)                            AS name_given_first
    ,CAST(NULL AS string)                            AS name_given_middle
    ,pgr.telecom_phone                               AS telecom_phone
    ,pgr.address_line1                               AS address_line1
    ,pgr.address_line2                               AS address_line2
    ,pgr.address_city                                AS address_city
    ,pgr.address_state                               AS address_state
    ,pgr.address_postalCode                          AS address_postalCode
    ,pgr.identifier_locationID                       AS practiceCode
    ,pgr.locationDesc                                AS practiceName
    ,pgr.identifier_tin                              AS poCode
    ,pgr.tier2Desc                                   AS poName
    ,CASE WHEN pgr.identifier_npi IS NULL THEN '' ELSE pgr.locationDesc END AS providerSpecialtyDescription
    ,CAST(NULL AS string)                            AS taxonomyCode1
    ,CAST(NULL AS string)                            AS hpSpecialtyCode1
    ,CAST(NULL AS string)                            AS advProviderSpecialtyCode1
    ,CAST(NULL AS string)                            AS taxonomyCode2
    ,CAST(NULL AS string)                            AS hpSpecialtyCode2
    ,CAST(NULL AS string)                            AS advProviderSpecialtyCode2
    ,CAST(NULL AS string)                            AS taxonomyCode3
    ,CAST(NULL AS string)                            AS hpSpecialtyCode3
    ,CAST(NULL AS string)                            AS advProviderSpecialtyCode3
    ,CAST(NULL AS string)                            AS taxonomyCode4
    ,CAST(NULL AS string)                            AS hpSpecialtyCode4
    ,CAST(NULL AS string)                            AS advProviderSpecialtyCode4
    ,CAST(NULL AS string)                            AS taxonomyCode5
    ,CAST(NULL AS string)                            AS hpSpecialtyCode5
    ,CAST(NULL AS string)                            AS advProviderSpecialtyCode5
    ,CAST(NULL AS string)                            AS extension_isPrescribePrivilege
    ,CAST(NULL AS string)                            AS identifier_providerDEA
    ,CAST(NULL AS string)                            AS identifier_payerID
    ,CAST(NULL AS string)                            AS extension_isContracted
    ,CAST(NULL AS string)                            AS extension_providerHAI
    ,CAST(NULL AS string)                            AS identifier_hospitalID
    ,CAST(NULL AS string)                            AS extension_isExcludedFromProviderReporting
    ,CAST(NULL AS string)                            AS identifier_alternateKey1
    ,CAST(NULL AS string)                            AS identifier_alternateKey2
    ,CAST(NULL AS string)                            AS identifier_alternateKey3
    ,CAST(NULL AS string)                            AS identifier_alternateKey4
    ,CAST(NULL AS string)                            AS identifier_alternateKey5
    ,CAST(NULL AS string)                            AS identifier_alternateKey6
    ,CAST(NULL AS string)                            AS identifier_alternateKey7
    ,CAST(NULL AS string)                            AS identifier_alternateKey8
    ,CAST(NULL AS string)                            AS identifier_alternateKey9
    ,CAST(NULL AS string)                            AS identifier_alternateKey10
    ,'Targeted'                                      AS extension_programType
    ,'New - Targeted'                                AS extension_practiceTargetedStatus
    ,CAST(NULL AS string)                            AS extension_ProductID
    ,CAST(NULL AS string)                            AS extension_ProviderType
    ,ROW_NUMBER() OVER(PARTITION BY pgr.identifier_providerID ORDER BY pgr.loadDateTime DESC) AS RowNumber
  FROM provider_hierarchy pgr
),
ProviderHierarchyFiltered AS (
  SELECT *
  FROM ProviderHierarchy
  WHERE RowNumber = 1
),
CombinedProvider AS (
  SELECT 
     p.ESAIInternalProviderID
    ,CURRENT_DATE() AS effectiveStartDate
    ,CAST(NULL AS date) AS effectiveEndDate
    ,1 AS isCurrent
    ,p.identifier_npi
    ,p.identifier_tin
    ,p.name_family
    ,p.name_given_first
    ,p.name_given_middle
    ,p.telecom_phone
    ,p.address_line1
    ,p.address_line2
    ,p.address_city
    ,p.address_state
    ,p.address_postalCode
    ,p.practiceCode
    ,p.practiceName
    ,p.poCode AS providerOrgCode
    ,p.poName AS providerOrgName
    ,p.providerSpecialtyDescription
    ,p.taxonomyCode1
    ,p.hpSpecialtyCode1
    ,p.advProviderSpecialtyCode1
    ,p.taxonomyCode2
    ,p.hpSpecialtyCode2
    ,p.advProviderSpecialtyCode2
    ,p.taxonomyCode3
    ,p.hpSpecialtyCode3
    ,p.advProviderSpecialtyCode3
    ,p.taxonomyCode4
    ,p.hpSpecialtyCode4
    ,p.advProviderSpecialtyCode4
    ,p.taxonomyCode5
    ,p.hpSpecialtyCode5
    ,p.advProviderSpecialtyCode5
    ,p.extension_isPrescribePrivilege
    ,p.identifier_providerDEA
    ,p.identifier_payerID
    ,p.extension_isContracted
    ,p.extension_providerHAI
    ,p.identifier_hospitalID
    ,p.extension_isExcludedFromProviderReporting
    ,p.identifier_alternateKey1
    ,p.identifier_alternateKey2
    ,p.identifier_alternateKey3
    ,p.identifier_alternateKey4
    ,p.identifier_alternateKey5
    ,p.identifier_alternateKey6
    ,p.identifier_alternateKey7
    ,p.identifier_alternateKey8
    ,p.identifier_alternateKey9
    ,p.identifier_alternateKey10
    ,p.extension_programType
    ,p.extension_practiceTargetedStatus
    ,p.extension_ProductID
    ,p.extension_ProviderType
  FROM ProviderHierarchyFiltered p
),
FinalProvider AS (
  SELECT 
     -- Generates an integer surrogate key to avoid casting errors against INT columns
     CAST(ABS(HASH(p.ESAIInternalProviderID, CURRENT_DATE())) AS INT) AS providerKey
    ,p.ESAIInternalProviderID
    ,p.effectiveStartDate
    ,p.effectiveEndDate
    ,p.isCurrent
    ,p.identifier_npi
    ,p.identifier_tin
    ,p.name_family
    ,p.name_given_first
    ,p.name_given_middle
    ,p.telecom_phone
    ,p.address_line1
    ,p.address_line2
    ,p.address_city
    ,p.address_state
    ,p.address_postalCode
    ,p.practiceCode
    ,p.practiceName
    ,p.providerOrgCode
    ,p.providerOrgName
    ,p.providerSpecialtyDescription
    ,p.taxonomyCode1
    ,p.hpSpecialtyCode1
    ,p.advProviderSpecialtyCode1
    ,p.taxonomyCode2
    ,p.hpSpecialtyCode2
    ,p.advProviderSpecialtyCode2
    ,p.taxonomyCode3
    ,p.hpSpecialtyCode3
    ,p.advProviderSpecialtyCode3
    ,p.taxonomyCode4
    ,p.hpSpecialtyCode4
    ,p.advProviderSpecialtyCode4
    ,p.taxonomyCode5
    ,p.hpSpecialtyCode5
    ,p.advProviderSpecialtyCode5
    ,p.extension_isPrescribePrivilege
    ,p.identifier_providerDEA
    ,p.identifier_payerID
    ,p.extension_isContracted
    ,p.extension_providerHAI
    ,p.identifier_hospitalID
    ,p.extension_isExcludedFromProviderReporting
    ,p.identifier_alternateKey1
    ,p.identifier_alternateKey2
    ,p.identifier_alternateKey3
    ,p.identifier_alternateKey4
    ,p.identifier_alternateKey5
    ,p.identifier_alternateKey6
    ,p.identifier_alternateKey7
    ,p.identifier_alternateKey8
    ,p.identifier_alternateKey9
    ,p.identifier_alternateKey10
    ,p.extension_programType
    ,p.extension_practiceTargetedStatus
    ,p.extension_ProductID
    ,p.extension_ProviderType
  FROM CombinedProvider p
)
SELECT * FROM FinalProvider;