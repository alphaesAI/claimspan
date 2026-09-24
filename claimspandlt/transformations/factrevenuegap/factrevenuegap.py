import dlt
from pyspark.sql.functions import col, lit, expr, sha2, concat_ws, lpad, substring, upper, trim, to_date

@dlt.table(
    name="factrevenuegap",
    comment="Fact revenue gap table combining silver revenue gaps with gold dimensions."
)
@dlt.expect_or_drop("valid_member_key", "memberKey IS NOT NULL AND memberKey <> -99")
def factrevenuegap():
    mrg = dlt.read("silver_revenue_gap")
    dim_month = dlt.read("gold_dimmonth")
    dim_client = dlt.read("gold_dimclient")
    dim_member = dlt.read("gold_dimmember").withColumn("memberKey", col("ESAIInternalPersonID"))
    dim_hcc = dlt.read("gold_dimhcc")
    dim_provider = dlt.read("gold_dimprovider")
    dim_alert = dlt.read("gold_dimalertgroup")
    dim_date1 = dlt.read("gold_dimdate")
    dim_date2 = dlt.read("gold_dimdate")
    dim_date3 = dlt.read("gold_dimdate")
    dim_date4 = dlt.read("gold_dimdate")

    return (
        mrg.alias("mrg")
        .join(
            dim_month.alias("dimMonth"),
            concat_ws("", col("dimMonth.yearNumber"), lpad(col("dimMonth.monthNumber"), 2, "0")) == 
            substring(concat_ws("", substring(col("mrg.reportPeriod"), 1, 4), substring(col("mrg.reportPeriod"), 5, 2)), 1, 6),
            "left"
        )
        .join(
            dim_client.alias("dimClient"),
            upper(trim(col("mrg.clientCode"))) == upper(trim(col("dimClient.clientCode"))),
            "left"
        )
        .join(
            dim_member.alias("dimMember"),
            (trim(col("mrg.memberIdentifier")) == trim(col("dimMember.identifier_planMemberID"))) & 
            (col("dimMember.isCurrentPMUP").cast("int") == 1),
            "left"
        )
        .join(
            dim_hcc.alias("dimHCC"),
            (trim(col("mrg.hccCode")) == trim(col("dimHCC.HCCNumber"))) &
            (substring(concat_ws("", substring(col("mrg.reportPeriod"), 1, 4), substring(col("mrg.reportPeriod"), 5, 2)), 1, 4) == col("dimHCC.EffectiveYear").cast("string")) &
            (trim(col("mrg.hccVersion")) == trim(col("dimHCC.HCCVersion"))) &
            upper(trim(col("dimHCC.HCCType"))).isin(["COMM", "ESRD", "RX"]),
            "left"
        )
        .join(
            dim_provider.alias("dimProvider"),
            (trim(col("mrg.practitionerIdentifier")) == trim(col("dimProvider.practitionerIdentifier"))) &
            (col("dimProvider.isCurrent").cast("int") == 1),
            "left"
        )
        .join(
            dim_alert.alias("dimAlertGroup"),
            trim(col("mrg.alertCategory")) == trim(col("dimAlertGroup.alertGroupCode")),
            "left"
        )
        .join(
            dim_date1.alias("dimDate1"),
            to_date(col("mrg.snapshotDate")) == col("dimDate1.date"),
            "left"
        )
        .join(
            dim_date2.alias("dimDate2"),
            to_date(col("mrg.lastDCConfirmedDate")) == col("dimDate2.date"),
            "left"
        )
        .join(
            dim_date3.alias("dimDate3"),
            to_date(col("mrg.lastPCPVisitDate")) == col("dimDate3.date"),
            "left"
        )
        .join(
            dim_date4.alias("dimDate4"),
            to_date(col("mrg.lastAWVDate")) == col("dimDate4.date"),
            "left"
        )
        .select(
            expr("coalesce(dimMonth.monthKey, -99)").alias("pecYearMonthKey"),
            expr("coalesce(dimClient.clientKey, -99)").alias("clientKey"),
            expr("coalesce(dimMember.memberKey, -99)").alias("memberKey"),
            lit("-99").alias("memberGroupKey"),
            expr("coalesce(mrg.payerIdentifier, '')").alias("planID"),
            expr("coalesce(dimProvider.payerIdentifier, '')").alias("payerIdentifier"),
            expr("coalesce(dimHCC.hccKey, -99)").alias("hccKey"),
            expr("coalesce(dimDate1.dateKey, -99)").alias("snapshotDateKey"),
            expr("coalesce(dimProvider.providerKey, -99)").alias("planProviderKey"),
            expr("coalesce(dimAlertGroup.alertGroupKey, -99)").alias("alertGroupKey"),
            expr("CASE WHEN mrg.closureReason IS NOT NULL THEN 'Y' ELSE 'N' END").alias("isHCCClosed"),
            expr("coalesce(dimDate2.dateKey, -99)").alias("lastDCConfirmedDateKey"),
            expr("coalesce(dimDate3.dateKey, -99)").alias("lastPCPVisitDateKey"),
            expr("coalesce(dimDate4.dateKey, -99)").alias("lastAWVDateKey"),
            sha2(concat_ws("|",
                expr("coalesce(dimMonth.monthKey, -99)"),
                expr("coalesce(dimMember.memberKey, -99)"),
                expr("coalesce(dimHCC.hccKey, -99)"),
                expr("coalesce(dimClient.clientKey, -99)")
            ), 256).alias("factRevenueGapHashKey"),
            sha2(concat_ws("|",
                expr("CASE WHEN mrg.closureReason IS NOT NULL THEN 'Y' ELSE 'N' END"),
                expr("coalesce(mrg.payerIdentifier, '')"),
                expr("coalesce(dimProvider.payerIdentifier, '')"),
                expr("coalesce(dimDate1.dateKey, -99)"),
                expr("coalesce(dimProvider.providerKey, -99)"),
                expr("coalesce(dimAlertGroup.alertGroupKey, -99)"),
                expr("coalesce(dimDate2.dateKey, -99)"),
                expr("coalesce(dimDate3.dateKey, -99)"),
                expr("coalesce(dimDate4.dateKey, -99)")
            ), 256).alias("fullRowHash"),
            expr("cast(date_format(current_date(), 'yyyyMMdd') as int)").alias("loadDateKey")
        )
    )