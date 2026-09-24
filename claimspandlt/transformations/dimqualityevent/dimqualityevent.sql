-- 1. PROCESSING: Define streaming view for quality events directly from silver_gapsincare
CREATE TEMPORARY STREAMING LIVE VIEW stg_quality_event_processed
AS SELECT 
    HASH(measureName) AS qualityEventKey,
    measureName AS qualityEventDescription
FROM STREAM(LIVE.silver_gapsincare)
WHERE measureName IS NOT NULL 
  AND measureName <> '';

-- 2. TARGET TABLE: Create target streaming Delta table for gold quality events
CREATE OR REFRESH STREAMING TABLE gold_dimqualityevent;

-- 3. CDC MERGE: Apply changes using SCD Type 1 deduplicating on description
APPLY CHANGES INTO LIVE.gold_dimqualityevent
FROM STREAM(LIVE.stg_quality_event_processed)
KEYS (qualityEventDescription)
SEQUENCE BY qualityEventKey
STORED AS SCD TYPE 1;