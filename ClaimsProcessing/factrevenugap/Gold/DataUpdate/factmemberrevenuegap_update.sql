MERGE INTO claimspan.gold.factmemberrevenuegap AS target
USING (
  WITH sourceTbl AS (
    SELECT
      IFNULL(dimMonth.monthKey, -99)                                  AS pecYearMonthKey,
      IFNULL(dimClient.clientKey, -99)                                AS clientKey,
      IFNULL(dimMember.memberKey, -99)                                AS memberKey,
      CAST('-99' AS STRING)                                           AS memberGroupKey,
      IFNULL(mrg.planID, '')                                          AS planID,
      IFNULL(dimProvider.payerIdentifier, '')                         AS payerIdentifier,
      IFNULL(dimHCC.hccKey, -99)                                      AS hccKey,
      IFNULL(dimDate1.dateKey, -99)                                   AS snapshotDateKey,
      IFNULL(dimProvider.providerKey, -99)                            AS planProviderKey,
      IFNULL(dimAlertGroup.alertGroupKey, -99)                        AS alertGroupKey,
      CASE WHEN mrg.closureReason IS NOT NULL THEN 'Y' ELSE 'N' END  AS isHCCClosed,
      IFNULL(dimDate2.dateKey, -99)                                   AS lastDCConfirmedDateKey,
      IFNULL(dimDate3.dateKey, -99)                                   AS lastPCPVisitDateKey,
      IFNULL(dimDate4.dateKey, -99)                                   AS lastAWVDateKey
    FROM memberRevenueGap mrg
    LEFT JOIN dimMonth dimMonth
      ON REPLACE(CAST(mrg.reportMonth AS STRING), '-', '') = CONCAT(CAST(dimMonth.yearNumber AS STRING), LPAD(CAST(dimMonth.monthNumber AS STRING), 2, '0'))
    LEFT JOIN dimClient dimClient
      ON UPPER(TRIM(mrg.clientCode)) = UPPER(TRIM(dimClient.clientCode))
    LEFT JOIN dimMember dimMember
      ON TRIM(mrg.planMemberID) = TRIM(dimMember.identifier_planMemberID)
     AND CAST(dimMember.isCurrent AS INT) = 1
    LEFT JOIN dimHCC dimHCC
      ON TRIM(mrg.hccNumber) = TRIM(dimHCC.HCCNumber)
     AND SUBSTRING(REPLACE(CAST(mrg.reportMonth AS STRING), '-', ''), 1, 4) = CAST(dimHCC.EffectiveYear AS STRING)
     AND TRIM(mrg.hccVersion) = TRIM(dimHCC.HCCVersion)
     AND UPPER(TRIM(dimHCC.HCCType)) IN ('COMM', 'ESRD', 'RX')
    LEFT JOIN dimProvider dimProvider
      ON TRIM(mrg.providerID) = TRIM(dimProvider.practitionerIdentifier)
     AND CAST(dimProvider.isCurrent AS INT) = 1
    LEFT JOIN dimAlertGroup dimAlertGroup
      ON TRIM(mrg.alertCategory) = TRIM(dimAlertGroup.alertGroupCode)
    LEFT JOIN dimDate dimDate1
      ON TO_DATE(mrg.snapshotDate) = dimDate1.date
    LEFT JOIN dimDate dimDate2
      ON TO_DATE(mrg.lastDCConfirmedDate) = dimDate2.date
    LEFT JOIN dimDate dimDate3
      ON TO_DATE(mrg.lastPCPVisitDate) = dimDate3.date
    LEFT JOIN dimDate dimDate4
      ON TO_DATE(mrg.lastAWVDate) = dimDate4.date
  )
  SELECT 
    pecYearMonthKey,
    clientKey,
    memberKey,
    memberGroupKey,
    planID,
    payerIdentifier,
    hccKey,
    snapshotDateKey,
    planProviderKey,
    alertGroupKey,
    isHCCClosed,
    lastDCConfirmedDateKey,
    lastPCPVisitDateKey,
    lastAWVDateKey,
    SHA2(CONCAT_WS('|', pecYearMonthKey, memberKey, hccKey, clientKey), 256) AS factMemberRevenueGapHashKey,
    SHA2(CONCAT_WS('|',
      isHCCClosed,
      planID,
      payerIdentifier,
      snapshotDateKey,
      planProviderKey,
      alertGroupKey,
      lastDCConfirmedDateKey,
      lastPCPVisitDateKey,
      lastAWVDateKey), 256) AS fullRowHash,
    CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT) AS loadDateKey
  FROM sourceTbl
) AS source
ON target.factMemberRevenueGapHashKey = source.factMemberRevenueGapHashKey
WHEN MATCHED AND target.fullRowHash <> source.fullRowHash THEN
  UPDATE SET
    target.pecYearMonthKey        = source.pecYearMonthKey,
    target.clientKey              = source.clientKey,
    target.memberKey              = source.memberKey,
    target.memberGroupKey         = source.memberGroupKey,
    target.planID                 = source.planID,
    target.payerIdentifier        = source.payerIdentifier,
    target.hccKey                 = source.hccKey,
    target.snapshotDateKey        = source.snapshotDateKey,
    target.planProviderKey        = source.planProviderKey,
    target.alertGroupKey          = source.alertGroupKey,
    target.isHCCClosed            = source.isHCCClosed,
    target.lastDCConfirmedDateKey = source.lastDCConfirmedDateKey,
    target.lastPCPVisitDateKey    = source.lastPCPVisitDateKey,
    target.lastAWVDateKey         = source.lastAWVDateKey,
    target.fullRowHash            = source.fullRowHash,
    target.loadDateKey            = source.loadDateKey
WHEN NOT MATCHED THEN
  INSERT (
    pecYearMonthKey,
    clientKey,
    memberKey,
    memberGroupKey,
    planID,
    payerIdentifier,
    hccKey,
    snapshotDateKey,
    planProviderKey,
    alertGroupKey,
    isHCCClosed,
    lastDCConfirmedDateKey,
    lastPCPVisitDateKey,
    lastAWVDateKey,
    factMemberRevenueGapHashKey,
    fullRowHash,
    loadDateKey
  )
  VALUES (
    source.pecYearMonthKey,
    source.clientKey,
    source.memberKey,
    source.memberGroupKey,
    source.planID,
    source.payerIdentifier,
    source.hccKey,
    source.snapshotDateKey,
    source.planProviderKey,
    source.alertGroupKey,
    source.isHCCClosed,
    source.lastDCConfirmedDateKey,
    source.lastPCPVisitDateKey,
    source.lastAWVDateKey,
    source.factMemberRevenueGapHashKey,
    source.fullRowHash,
    source.loadDateKey
  );