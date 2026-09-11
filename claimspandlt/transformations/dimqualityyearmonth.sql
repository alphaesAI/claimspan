-- Quality year-month dimension table
CREATE OR REFRESH STREAMING TABLE gold_dimqualityyearmonth
COMMENT "Quality year-month dimension table"
AS
SELECT DISTINCT
    hash(concat(left(evaluationDate, 4), '|', substring(evaluationDate, 6, 2))) as qualityYearMonthKey,
    cast(substring(evaluationDate, 6, 2) as int) as monthNumber,
    case substring(evaluationDate, 6, 2)
        when '01' then 'January' when '02' then 'February' when '03' then 'March'
        when '04' then 'April' when '05' then 'May' when '06' then 'June'
        when '07' then 'July' when '08' then 'August' when '09' then 'September'
        when '10' then 'October' when '11' then 'November' when '12' then 'December'
        else 'Unknown' end as monthName,
    cast(left(evaluationDate, 4) as int) as yearNumber,
    false as isRunout
FROM STREAM silver_gapsincare
WHERE evaluationDate IS NOT NULL