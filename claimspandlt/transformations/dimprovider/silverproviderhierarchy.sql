-- Silver Provider Hierarchy Layer
-- Processes provider hierarchy relationships across tiers (locations, groups)

CREATE OR REFRESH STREAMING TABLE silver_provider_hierarchy
COMMENT 'Provider Hierarchy - Multi-tier provider location and organizational relationships'
AS
SELECT 
  -- Provider identification
  identifier_providerID AS ProviderID,
  name_family AS ProviderLastName,
  identifier_npi AS ProviderNPI,
  
  -- Location (Tier 1) information
  identifier_locationGroupID AS LocationGroupID,
  extension_locationRanking AS LocationRanking,
  identifier_locationIDType AS LocationIDType,
  identifier_locationID AS LocationID,
  extension_locationDesc AS LocationDesc,
  identifier_locationTIN AS LocationTIN,
  address_line1 AS LocationAddress1,
  address_line2 AS LocationAddress2,
  address_city AS LocationCity,
  address_state AS LocationState,
  address_postalCode AS LocationZip,
  address_district AS CountyCode,
  CAST(telecom_phone AS BIGINT) AS PhoneNumber,
  CAST(telecom_fax AS BIGINT) AS FaxNumber,
  CAST(contact_name AS STRING) AS ContactPerson,
  extension_doNotChase AS DoNotChase,
  
  -- Tier 2 information
  identifier_tier2IDType AS Tier2IDType,
  identifier_tier2ID AS Tier2ID,
  extension_tier2Desc AS Tier2Desc,
  tier2_address_line1 AS Tier2Address1,
  tier2_address_line2 AS Tier2Address2,
  tier2_address_city AS Tier2City,
  tier2_address_state AS Tier2State,
  tier2_address_postalCode AS Tier2Zip,
  
  -- Tier 3 information
  identifier_tier3IDType AS Tier3IDType,
  identifier_tier3ID AS Tier3ID,
  extension_tier3Desc AS Tier3Desc,
  tier3_address_line1 AS Tier3Address1,
  tier3_address_line2 AS Tier3Address2,
  tier3_address_city AS Tier3City,
  tier3_address_state AS Tier3State,
  tier3_address_postalCode AS Tier3Zip,
  
  -- Tier 4 information
  identifier_tier4IDType AS Tier4IDType,
  identifier_tier4ID AS Tier4ID,
  extension_tier4Desc AS Tier4Desc,
  tier4_address_line1 AS Tier4Address1,
  tier4_address_line2 AS Tier4Address2,
  tier4_address_city AS Tier4City,
  tier4_address_state AS Tier4State,
  tier4_address_postalCode AS Tier4Zip,
  
  -- Effective dates
  CAST(period_start AS DATE) AS StartDate,
  CAST(period_end AS DATE) AS EndDate,
  
  -- File metadata
  FileID,
  FileLayoutID,
  ClientID,
  LoadDateTime

FROM STREAM(provider_hierarchy_consolidated);
     