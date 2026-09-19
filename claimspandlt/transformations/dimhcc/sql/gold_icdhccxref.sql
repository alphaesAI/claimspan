-- Gold ICD-HCC Xref: Join 5 operational tables for ICD-to-HCC crosswalk
SELECT DISTINCT 
  XR.ICD AS icd, 
  IC.CodeType AS icdCodeType, 
  IY.EffectiveYear AS icdEffectiveYear, 
  HC.hccNumber, 
  HC.hccVersion, 
  HC.hccType, 
  HY.EffectiveYear AS hccEffectiveYear, 
  XR.isPrimary, 
  XR.EffectiveDateStart AS effectiveStartDate, 
  XR.EffectiveDateEnd AS effectiveEndDate, 
  cast(hash(concat(
    coalesce(XR.ICD, ''), '|', 
    coalesce(IC.CodeType, ''), '|', 
    coalesce(cast(IY.EffectiveYear as string), ''), '|', 
    coalesce(HC.hccNumber, ''), '|', 
    coalesce(HC.hccVersion, ''), '|', 
    coalesce(HC.hccType, ''), '|', 
    coalesce(cast(HY.EffectiveYear as string), ''), '|', 
    coalesce(cast(XR.isPrimary as string), 'false'), '|', 
    coalesce(cast(XR.EffectiveDateStart as string), ''), '|', 
    coalesce(cast(XR.EffectiveDateEnd as string), '')
  )) as bigint) AS icdHCCKey 
FROM HCCICDDatasetXref XR 
JOIN ICDDataset IC ON IC.ICDDatasetID = XR.ICDDatasetID 
JOIN ICDEffectiveYear IY ON IC.ICDEffectiveYearID = IY.ICDEffectiveYearID 
JOIN HCCDataset HC ON XR.HCCDatasetID = HC.HCCDatasetID 
  AND UPPER(HC.HCCType) IN ('COMM', 'ESRD') 
JOIN HCCEffectiveYear HY ON HC.HCCEffectiveYearID = HY.HCCEffectiveYearID 
ORDER BY icd, effectiveStartDate, effectiveEndDate, HC.hccNumber