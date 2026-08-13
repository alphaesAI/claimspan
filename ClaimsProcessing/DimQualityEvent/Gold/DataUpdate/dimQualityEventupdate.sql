MERGE INTO DestinationTable t 
USING (
    SELECT * FROM tempSQLScript
) s
ON IFNULL(s.qualityEventDescription, '') = IFNULL(t.qualityEventDescription, '') 

WHEN NOT MATCHED THEN 
    INSERT (
        qualityEventKey, 
        qualityEventDescription
    )
    VALUES (
        s.qualityEventKey, 
        s.qualityEventDescription
    );