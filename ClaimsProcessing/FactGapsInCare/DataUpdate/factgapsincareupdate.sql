MERGE INTO claimsprocessing.gold.factgapsincare t
USING (
    SELECT * FROM tempSQLScript
) s
ON s.gapsInCareID = t.gapsInCareID    

WHEN MATCHED AND s.fullRowHash <> t.fullRowHash THEN 
    UPDATE SET
        t.qualityYearMonthKey = s.qualityYearMonthKey,
        t.memberKey = s.memberKey,
        t.qualityMeasureKey = s.qualityMeasureKey,
        t.qualityEventKey = s.qualityEventKey,
        t.numerCnt = s.numerCnt,
        t.denomCnt = s.denomCnt,
        t.dateOfServiceDateKey = s.dateOfServiceDateKey,
        t.gapsEventProviderKey = s.gapsEventProviderKey,
        t.expectedRate = s.expectedRate,
        t.serviceNeededByDateKey = s.serviceNeededByDateKey,
        t.pdc = s.pdc,
        t.lastHBVal = s.lastHBVal,
        t.lastHBDateKey = s.lastHBDateKey,
        t.lastBPDia = s.lastBPDia,
        t.lastBPSys = s.lastBPSys,
        t.claimNumber = s.claimNumber,
        t.hbTest = s.hbTest,
        t.fullRowHash = s.fullRowHash

WHEN NOT MATCHED THEN 
    INSERT (
        gapsInCareID,
        qualityYearMonthKey,
        memberKey,
        qualityMeasureKey,
        qualityEventKey,
        numerCnt,
        denomCnt,
        dateOfServiceDateKey,
        gapsEventProviderKey,
        expectedRate,
        serviceNeededByDateKey,
        pdc,
        lastHBVal,
        lastHBDateKey,
        lastBPDia,
        lastBPSys,
        claimNumber,
        hbTest,
        fullRowHash
    )
    VALUES (
        s.gapsInCareID,
        s.qualityYearMonthKey,
        s.memberKey,
        s.qualityMeasureKey,
        s.qualityEventKey,
        s.numerCnt,
        s.denomCnt,
        s.dateOfServiceDateKey,
        s.gapsEventProviderKey,
        s.expectedRate,
        s.serviceNeededByDateKey,
        s.pdc,
        s.lastHBVal,
        s.lastHBDateKey,
        s.lastBPDia,
        s.lastBPSys,
        s.claimNumber,
        s.hbTest,
        s.fullRowHash
    );