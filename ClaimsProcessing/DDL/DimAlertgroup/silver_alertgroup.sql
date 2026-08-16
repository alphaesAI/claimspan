CREATE TABLE IF NOT EXISTS claimspan.silver.silver_alertgroup (
 alertGroupID  int
,alertGroupCode  string
,alertGroupDescription  string
,displayText  string
,sortOrder  int
,isActive  boolean
,hashKey  int
) USING delta; 
