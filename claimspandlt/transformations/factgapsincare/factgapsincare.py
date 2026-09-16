import dlt
from pyspark.sql.functions import col, lit, expr, sha2, concat_ws, lpad, substring

@dlt.table(
    name="factgapsincare",
    comment="Fact gaps in care table combining source metrics and dimensions"
)
@dlt.expect_or_drop("valid_gapsincare_id", "gapsInCareID IS NOT NULL")
def factgapsincare():
    gic = dlt.read_stream("silver_gapsincare")
    dim_ym = dlt.read("gold_dimqualityyearmonth")
    dim_mem = dlt.read("gold_dimmember")
    dim_meas = dlt.read("gold_dimqualitymeasure")
    dim_event = dlt.read("gold_dimqualityevent")
    dim_date1 = dlt.read("gold_dimdate")
    dim_date2 = dlt.read("gold_dimdate")
    dim_date3 = dlt.read("gold_dimdate")

    return (
        gic.alias("gic")
        .join(
            dim_ym.alias("dimQualityYearMonth"),
            concat_ws("", substring(col("gic.evaluationDate"), 1, 4), lpad(substring(col("gic.evaluationDate"), 6, 2), 2, "0")) == concat_ws("", col("dimQualityYearMonth.yearNumber"), lpad(col("dimQualityYearMonth.monthNumber"), 2, "0")),
            "left"
        )
        .join(
            dim_mem.alias("dimMember"),
            (col("gic.identifier_planMemberID") == col("dimMember.identifier_planMemberID")) & (col("dimMember.IsCurrentPMUP") == True),
            "left"
        )
        .join(
            dim_meas.alias("gold_dimQualityMeasure"),
            (col("gic.measureSource") == col("gold_dimQualityMeasure.qualityMeasureCode")) & 
            (substring(col("gic.evaluationDate"), 1, 4) == col("gold_dimQualityMeasure.measureYear")) & 
            (col("gic.measureName") == col("gold_dimQualityMeasure.qualityMeasureName")),
            "left"
        )
        .join(
            dim_event.alias("gold_dimQualityEvent"),
            col("gic.measureName") == col("gold_dimQualityEvent.qualityEventDescription"),
            "left"
        )
        .join(
            dim_date1.alias("dimDate1"),
            col("gic.dateOfService") == col("dimDate1.date"),
            "left"
        )
        .join(
            dim_date2.alias("dimDate2"),
            col("gic.serviceNeededByDate") == col("dimDate2.date"),
            "left"
        )
        .join(
            dim_date3.alias("dimDate3"),
            col("gic.lastHBDate") == col("dimDate3.date"),
            "left"
        )
        .select(
            col("gic.gapsInCareID"),
            expr("coalesce(dimQualityYearMonth.qualityYearMonthKey, -1)").alias("qualityYearMonthKey"),
            expr("coalesce(dimMember.HashKey, -1)").alias("memberKey"),
            expr("coalesce(gold_dimQualityMeasure.qualityMeasureKey, -1)").alias("qualityMeasureKey"),
            expr("coalesce(gold_dimQualityEvent.qualityEventKey, -1)").alias("qualityEventKey"),
            col("gic.numerCnt"),
            col("gic.denomCnt"),
            expr("coalesce(dimDate1.dateKey, -1)").alias("dateOfServiceDateKey"),
            col("gic.practitioner_identifier").alias("gapsEventProviderKey"),
            col("gic.expectedRate"),
            expr("coalesce(dimDate2.dateKey, -1)").alias("serviceNeededByDateKey"),
            col("gic.PDC").alias("pdc"),
            col("gic.lastHBVal"),
            expr("coalesce(dimDate3.dateKey, -1)").alias("lastHBDateKey"),
            col("gic.lastBPDia"),
            col("gic.lastBPSys"),
            col("gic.identifier_claimnumber"),
            col("gic.hbTest"),
            sha2(concat_ws("|",
                expr("coalesce(dimQualityYearMonth.qualityYearMonthKey, -1)"),
                expr("coalesce(dimMember.HashKey, -1)"),
                expr("coalesce(gold_dimQualityMeasure.qualityMeasureKey, -1)"),
                expr("coalesce(gold_dimQualityEvent.qualityEventKey, -1)"),
                expr("coalesce(cast(gic.numerCnt as string), '|')"),
                expr("coalesce(cast(gic.denomCnt as string), '|')"),
                expr("coalesce(dimDate1.dateKey, -1)"),
                expr("coalesce(cast(gic.practitioner_identifier as string), '|')"),
                expr("coalesce(cast(gic.expectedRate as string), '|')"),
                expr("coalesce(dimDate2.dateKey, -1)"),
                expr("coalesce(cast(gic.PDC as string), '|')"),
                expr("coalesce(cast(gic.lastHBVal as string), '|')"),
                expr("coalesce(dimDate3.dateKey, -1)"),
                expr("coalesce(cast(gic.lastBPDia as string), '|')"),
                expr("coalesce(cast(gic.lastBPSys as string), '|')"),
                expr("coalesce(cast(gic.identifier_claimnumber as string), '|')"),
                expr("coalesce(cast(gic.hbTest as string), '|')")
            ), 256).alias("fullRowHash")
        )
    )