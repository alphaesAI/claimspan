-- Silver Provider Hierarchy Layer
-- Processes provider hierarchy relationships across tiers (locations, groups)

CREATE OR REFRESH STREAMING TABLE silver_provider_hierarchy
COMMENT 'Provider Hierarchy - Multi-tier provider location and organizational relationships'
AS
SELECT 
  -- Provider identification
  PROVIDERID AS ProviderID,
  PROVIDERLASTNAME AS ProviderLastName,
  PROVIDERNPI AS ProviderNPI,
  
  -- Location (Tier 1) information
  LOCATIONGROUPID AS LocationGroupID,
  LOCATIONRANKING AS LocationRanking,
  LOCATIONIDTYPE AS LocationIDType,
  LOCATIONID AS LocationID,
  LOCATIONDESC AS LocationDesc,
  LOCATIONTIN AS LocationTIN,
  LOCATIONADDRESS1 AS LocationAddress1,
  LOCATIONADDRESS2 AS LocationAddress2,
  LOCATIONCITY AS LocationCity,
  LOCATIONSTATE AS LocationState,
  LOCATIONZIP AS LocationZip,
  COUNTYCODE AS CountyCode,
  CAST(PHONENUMBER AS BIGINT) AS PhoneNumber,
  CAST(FAXNUMBER AS BIGINT) AS FaxNumber,
  CAST(CONTACTPERSON AS STRING) AS ContactPerson,
  DONOTCHASE AS DoNotChase,
  
  -- Tier 2 information
  TIER2IDTYPE AS Tier2IDType,
  TIER2ID AS Tier2ID,
  TIER2DESC AS Tier2Desc,
  TIER2ADDRESS1 AS Tier2Address1,
  TIER2ADDRESS2 AS Tier2Address2,
  TIER2CITY AS Tier2City,
  TIER2STATE AS Tier2State,
  TIER2ZIP AS Tier2Zip,
  
  -- Tier 3 information
  TIER3IDTYPE AS Tier3IDType,
  TIER3ID AS Tier3ID,
  TIER3DESC AS Tier3Desc,
  TIER3ADDRESS1 AS Tier3Address1,
  TIER3ADDRESS2 AS Tier3Address2,
  TIER3CITY AS Tier3City,
  TIER3STATE AS Tier3State,
  TIER3ZIP AS Tier3Zip,
  
  -- Tier 4 information
  TIER4IDTYPE AS Tier4IDType,
  TIER4ID AS Tier4ID,
  TIER4DESC AS Tier4Desc,
  TIER4ADDRESS1 AS Tier4Address1,
  TIER4ADDRESS2 AS Tier4Address2,
  TIER4CITY AS Tier4City,
  TIER4STATE AS Tier4State,
  TIER4ZIP AS Tier4Zip,
  
  -- Effective dates
  CAST(STARTDATE AS DATE) AS StartDate,
  CAST(ENDDATE AS DATE) AS EndDate,
  
  -- File metadata
  FileID,
  FileLayoutID,
  ClientID,
  LoadDateTime

FROM STREAM(provider_hierarchy_consolidated);
     