-- Silver HCC: Join HCCDataset with HCCEffectiveYear, deduplicate by latest CreationDatetime
WITH THIS AS (
  SELECT 
    ds.HCCNumber, 
    ds.HCCDescription, 
    ds.HCCVersion, 
    ds.HCCType, 
    ds.IsChronic, 
    ey.EffectiveYear, 
    ey.EffectiveDateStart, 
    ey.EffectiveDateEnd, 
    hash(concat(
      coalesce(cast(ds.HCCNumber as string), '|'), '|', 
      coalesce(cast(ds.HCCDescription as string), '|'), '|', 
      coalesce(cast(ds.HCCVersion as string), '|'), '|', 
      coalesce(cast(ds.HCCType as string), '|'), '|', 
      coalesce(cast(ds.IsChronic as string), 'false'), '|', 
      coalesce(cast(ey.EffectiveYear as string), '|'), '|', 
      coalesce(cast(ey.EffectiveDateStart as string), '|'), '|', 
      coalesce(cast(ey.EffectiveDateEnd as string), '|')
    )) AS hashKey, 
    ROW_NUMBER() OVER(
      PARTITION BY 
        coalesce(cast(ds.HCCNumber as string), '|'), 
        coalesce(cast(ds.HCCVersion as string), '|'), 
        coalesce(cast(ds.HCCType as string), '|'), 
        coalesce(cast(ey.EffectiveYear as string), '|') 
      ORDER BY coalesce(cast(ds.CreationDatetime as string), '9999-12-31') DESC
    ) AS RowNumber 
  FROM HCCDataSet ds 
  LEFT JOIN HCCEffectiveYear ey ON ds.HCCEffectiveYearID = ey.HCCEffectiveYearID
) 
SELECT 
  HCCNumber, HCCDescription, HCCVersion, HCCType, IsChronic, 
  EffectiveYear, EffectiveDateStart, EffectiveDateEnd, hashKey 
FROM THIS 
WHERE RowNumber = 1