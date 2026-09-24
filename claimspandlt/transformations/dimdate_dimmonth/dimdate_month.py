import dlt
from pyspark.sql.functions import (
    col, lit, expr, explode, sequence, date_format, 
    year, quarter, month, weekofyear, dayofweek, dayofyear, concat
)

@dlt.table(
    name="gold_dimdate",
    comment="Calendar date dimension table"
)
def gold_dimdate():
    base_df = spark.createDataFrame([("1900-01-01", "9999-12-31")], ["startDate", "endDate"]) \
        .select(col("startDate").cast("date"), col("endDate").cast("date"))
    
    dates_df = base_df.withColumn("newDate", explode(sequence(col("startDate"), col("endDate"), expr("interval 1 day"))))
    
    return dates_df.select(
        date_format(col("newDate"), "yyyyMMdd").cast("integer").alias("dateKey"),
        date_format(col("newDate"), "yyyy-MM-dd HH:mm:ss").cast("date").alias("date"),
        date_format(col("newDate"), "MMM d, yyyy").alias("shortDateName"),
        date_format(col("newDate"), "MMMM d, yyyy").alias("longDateName"),
        year(col("newDate")).alias("yearNumber"),
        date_format(col("newDate"), "yyyy").alias("yearName"),
        concat(year(col("newDate")), quarter(col("newDate"))).cast("integer").alias("quarterKey"),
        quarter(col("newDate")).alias("quarterNumber"),
        concat(lit("Q"), quarter(col("newDate"))).alias("quarterName"),
        quarter(col("newDate")).alias("quarterOfYearNumber"),
        concat(lit("Q"), quarter(col("newDate")), lit(", "), year(col("newDate"))).alias("quarterOfYearName"),
        date_format(col("newDate"), "yyyyMM").cast("integer").alias("monthKey"),
        month(col("newDate")).alias("monthNumber"),
        date_format(col("newDate"), "MMMM").alias("monthName"),
        expr("case when month(newDate) in (1,4,7,10) then 1 when month(newDate) in (2,5,8,11) then 2 else 3 end").alias("monthOfQuarterNumber"),
        concat(lit("Month "), expr("case when month(newDate) in (1,4,7,10) then 1 when month(newDate) in (2,5,8,11) then 2 else 3 end")).alias("monthOfQuarterName"),
        concat(date_format(col("newDate"), "MMM"), lit("-"), date_format(col("newDate"), "yyyy")).alias("monthOfYearShortName"),
        ((year(col("newDate")) * 1000) + weekofyear(col("newDate"))).alias("weekKey"),
        weekofyear(col("newDate")).alias("weekNumber"),
        concat(lit("Week "), weekofyear(col("newDate"))).alias("weekName"),
        dayofweek(col("newDate")).alias("dayOfWeekNumber"),
        date_format(col("newDate"), "EEEE").alias("dayOfWeekName"),
        dayofyear(col("newDate")).alias("dayOfYear"),
        expr("case when dayofweek(newDate) in (1,7) then false else true end").alias("isWorkDay")
    )

@dlt.table(
    name="gold_dimmonth",
    comment="Calendar month dimension table"
)
def gold_dimmonth():
    base_df = spark.createDataFrame([("1900-01-01", "9999-12-31")], ["startDate", "endDate"]) \
        .select(col("startDate").cast("date"), col("endDate").cast("date"))
    
    months_df = base_df.withColumn("newDate", explode(sequence(col("startDate"), col("endDate"), expr("interval 1 month"))))
    
    return months_df.select(
        date_format(col("newDate"), "yyyyMM").cast("integer").alias("monthKey"),
        month(col("newDate")).alias("monthNumber"),
        date_format(col("newDate"), "MMMM").alias("monthName"),
        year(col("newDate")).alias("yearNumber"),
        date_format(col("newDate"), "yyyy").alias("yearName"),
        quarter(col("newDate")).alias("quarterNumber"),
        concat(lit("Q"), quarter(col("newDate"))).alias("quarterName")
    )