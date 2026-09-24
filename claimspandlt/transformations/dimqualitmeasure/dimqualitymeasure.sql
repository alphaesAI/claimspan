CREATE TEMPORARY STREAMING LIVE VIEW stg_quality_measure_processed
AS SELECT 
    HASH(CONCAT(measureYear, '|', qualityMeasureCode, '|', qualityMeasureName)) AS qualityMeasureKey,
    measureYear,
    qualityMeasureCode,
    qualityMeasureName
FROM (
    SELECT 
        CAST(LEFT(evaluationDate, 4) AS INT) AS measureYear, 
        measureSource AS qualityMeasureCode, 
        measureName AS qualityMeasureName  
    FROM STREAM(LIVE.silver_gapsincare)
)
WHERE measureYear IS NOT NULL 
  AND IFNULL(qualityMeasureCode, '') <> '' 
  AND IFNULL(qualityMeasureName, '') <> '';

CREATE OR REFRESH STREAMING TABLE gold_dimqualitymeasure;

APPLY CHANGES INTO LIVE.gold_dimqualitymeasure
FROM STREAM(LIVE.stg_quality_measure_processed)
KEYS (measureYear, qualityMeasureCode, qualityMeasureName)
SEQUENCE BY measureYear
STORED AS SCD TYPE 1;