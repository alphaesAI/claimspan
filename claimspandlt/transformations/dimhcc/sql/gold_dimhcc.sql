-- Gold dimHCC: Project silver_hcc with hccKey hash for merge deduplication
SELECT 
  h.HCCNumber, 
  h.HCCDescription, 
  h.HCCVersion, 
  h.HCCType, 
  h.IsChronic, 
  h.EffectiveYear, 
  h.EffectiveDateStart, 
  h.EffectiveDateEnd, 
  h.hashKey, 
  hash(concat(
    coalesce(cast(h.HCCNumber as string), '|'), '|', 
    coalesce(cast(h.HCCVersion as string), '|'), '|', 
    coalesce(cast(h.HCCType as string), '|'), '|', 
    coalesce(cast(h.EffectiveYear as string), '|')
  )) AS hccKey 
FROM hcc h