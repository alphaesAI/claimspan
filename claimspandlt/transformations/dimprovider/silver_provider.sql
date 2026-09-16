-- Silver Provider Layer
-- Enriches consolidated provider data with reference lookups and quality checks

-- Silver Provider Person Bridge
-- Creates unique provider identifiers and tracks provider linkages
CREATE OR REFRESH STREAMING TABLE silver_provider_person_bridge
COMMENT 'Provider Person Bridge - Links provider identifiers (NPI, ProviderID, PayerID)'
AS
SELECT 
  -- Generate unique internal provider ID
  sha2(concat_ws('||', 
    COALESCE(identifier_providerID, ''),
    COALESCE(identifier_npi, ''),
    COALESCE(name_family, ''),
    COALESCE(name_given_first, '')
  ), 256) AS ESAIInternalProviderID,
  
  1 AS isCurrent,
  
  -- Unique record hash
  sha2(concat_ws('||',
    CAST(FileID AS STRING),
    COALESCE(identifier_providerID, ''),
    COALESCE(identifier_npi, '')
  ), 256) AS uniqueRecord,
  
  CAST(FileLayoutID AS INT) AS FileLayoutID,
  CAST(FileID AS BIGINT) AS FileID,
  
  -- Provider identifiers
  name_family,
  name_given_first,
  identifier_npi,
  identifier_providerDEA,
  identifier_payerID,
  identifier_providerID,
  
  -- Hash key for tracking
  sha2(concat_ws('||',
    COALESCE(identifier_providerID, ''),
    COALESCE(identifier_npi, '')
  ), 256) AS hashKey,
  
  1 AS isCurrentProviderID,
  1 AS isCurrentNPI,
  1 AS isOriginalProviderID,
  
  -- Provider-Member-Unique-Pair (PMUP) for linkage
  COALESCE(identifier_providerID, identifier_npi) AS pmup,
  1 AS isCurrentPMUP

FROM STREAM(provider_consolidated);


-- Silver Provider Main
-- Enriches provider data with taxonomy, specialty codes, and reference data
CREATE OR REFRESH STREAMING TABLE silver_provider
COMMENT 'Silver Provider - Enriched provider dimension with specialty codes and taxonomy'
AS
WITH consolidateProvider1 AS (
  SELECT 
    brdg.ESAIInternalProviderID,
    brdg.uniqueRecord,
    prov.ClientID,
    prov.FileID,
    prov.LoadDateTime,
    prov.FileLayoutID,
    prov.FileLayoutDescription,
    
    -- Provider identifiers
    prov.identifier_providerID,
    prov.name_family,
    prov.name_given_middle_initial,
    prov.name_given_first,
    
    -- Taxonomy and specialty codes (5 sets)
    COALESCE(prov.identifier_taxonomyCode1, cpt1.Taxo) AS TaxonomyCode1,
    COALESCE(s1.Specialty, prov.identifier_hpSpecialtyCode1) AS HpSpecialtyCode1,
    COALESCE(LPAD(CAST(s1.CMSSpecialtyCode AS STRING), 2, '0'), prov.identifier_advProviderSpecialtyCode1) AS ADVProviderSpecialtyCode1,
    
    COALESCE(prov.identifier_taxonomyCode2, cpt2.Taxo) AS TaxonomyCode2,
    COALESCE(s2.Specialty, prov.identifier_hpSpecialtyCode2) AS HpSpecialtyCode2,
    COALESCE(LPAD(CAST(s2.CMSSpecialtyCode AS STRING), 2, '0'), prov.identifier_advProviderSpecialtyCode2) AS ADVProviderSpecialtyCode2,
    
    COALESCE(prov.identifier_taxonomyCode3, cpt3.Taxo) AS TaxonomyCode3,
    COALESCE(s3.Specialty, prov.identifier_hpSpecialtyCode3) AS HpSpecialtyCode3,
    COALESCE(LPAD(CAST(s3.CMSSpecialtyCode AS STRING), 2, '0'), prov.identifier_advProviderSpecialtyCode3) AS ADVProviderSpecialtyCode3,
    
    COALESCE(prov.identifier_taxonomyCode4, cpt4.Taxo) AS TaxonomyCode4,
    COALESCE(s4.Specialty, prov.identifier_hpSpecialtyCode4) AS HpSpecialtyCode4,
    COALESCE(LPAD(CAST(s4.CMSSpecialtyCode AS STRING), 2, '0'), prov.identifier_advProviderSpecialtyCode4) AS ADVProviderSpecialtyCode4,
    
    COALESCE(prov.identifier_taxonomyCode5, cpt5.Taxo) AS TaxonomyCode5,
    COALESCE(s5.Specialty, prov.identifier_hpSpecialtyCode5) AS HpSpecialtyCode5,
    COALESCE(LPAD(CAST(s5.CMSSpecialtyCode AS STRING), 2, '0'), prov.identifier_advProviderSpecialtyCode5) AS ADVProviderSpecialtyCode5,
    
    -- Extensions
    prov.extension_prescribePrivilege,
    prov.identifier_payerID,
    prov.extension_contracted,
    prov.identifier_providerHAI,
    prov.identifier_hospitalID,
    prov.extension_excludeFromProviderReporting,
    
    -- Alternative reporting IDs
    prov.identifier_altProvReporting1,
    prov.identifier_altProvReporting2,
    prov.identifier_altProvReporting3,
    prov.identifier_altProvReporting4,
    prov.identifier_altProvReporting5,
    prov.identifier_altProvReporting6,
    prov.identifier_altProvReporting7,
    prov.identifier_altProvReporting8,
    prov.identifier_altProvReporting9,
    prov.identifier_altProvReporting10
    
  FROM STREAM(provider_consolidated) prov
  INNER JOIN STREAM(silver_provider_person_bridge) brdg
    ON prov.identifier_providerID = brdg.identifier_providerID
    
  -- Taxonomy lookups (5 sets)
  LEFT JOIN `claimspan`.silver.ref_careprecise_taxonomy s1
    ON prov.identifier_hpSpecialtyCode1 = s1.Specialty
  LEFT JOIN `claimspan`.silver.ref_credentialing cpt1
    ON s1.Taxo = cpt1.Taxo
    
  LEFT JOIN `claimspan`.silver.ref_careprecise_taxonomy s2
    ON prov.identifier_hpSpecialtyCode2 = s2.Specialty
  LEFT JOIN `claimspan`.silver.ref_credentialing cpt2
    ON s2.Taxo = cpt2.Taxo
    
  LEFT JOIN `claimspan`.silver.ref_careprecise_taxonomy s3
    ON prov.identifier_hpSpecialtyCode3 = s3.Specialty
  LEFT JOIN `claimspan`.silver.ref_credentialing cpt3
    ON s3.Taxo = cpt3.Taxo
    
  LEFT JOIN `claimspan`.silver.ref_careprecise_taxonomy s4
    ON prov.identifier_hpSpecialtyCode4 = s4.Specialty
  LEFT JOIN `claimspan`.silver.ref_credentialing cpt4
    ON s4.Taxo = cpt4.Taxo
    
  LEFT JOIN `claimspan`.silver.ref_careprecise_taxonomy s5
    ON prov.identifier_hpSpecialtyCode5 = s5.Specialty
  LEFT JOIN `claimspan`.silver.ref_credentialing cpt5
    ON s5.Taxo = cpt5.Taxo
)

SELECT 
  ESAIInternalProviderID,
  uniqueRecord,
  ClientID,
  FileID,
  LoadDateTime,
  FileLayoutID,
  FileLayoutDescription,
  identifier_providerID,
  name_family AS LastName,
  name_given_first AS FirstName,
  name_given_middle_initial AS MiddleInitial,
  
  -- Taxonomy codes
  TaxonomyCode1, HpSpecialtyCode1, ADVProviderSpecialtyCode1,
  TaxonomyCode2, HpSpecialtyCode2, ADVProviderSpecialtyCode2,
  TaxonomyCode3, HpSpecialtyCode3, ADVProviderSpecialtyCode3,
  TaxonomyCode4, HpSpecialtyCode4, ADVProviderSpecialtyCode4,
  TaxonomyCode5, HpSpecialtyCode5, ADVProviderSpecialtyCode5,
  
  -- Extensions and identifiers
  extension_prescribePrivilege AS PrescribePrivilege,
  identifier_payerID AS PayorID,
  extension_contracted AS Contracted,
  identifier_providerHAI AS ProviderHAI,
  identifier_hospitalID AS HospitalID,
  extension_excludeFromProviderReporting AS ExcludeFromProviderReporting,
  
  identifier_altProvReporting1 AS AltProvReporting1,
  identifier_altProvReporting2 AS AltProvReporting2,
  identifier_altProvReporting3 AS AltProvReporting3,
  identifier_altProvReporting4 AS AltProvReporting4,
  identifier_altProvReporting5 AS AltProvReporting5,
  identifier_altProvReporting6 AS AltProvReporting6,
  identifier_altProvReporting7 AS AltProvReporting7,
  identifier_altProvReporting8 AS AltProvReporting8,
  identifier_altProvReporting9 AS AltProvReporting9,
  identifier_altProvReporting10 AS AltProvReporting10

FROM consolidateProvider1;
