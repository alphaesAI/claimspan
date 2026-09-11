import dlt

@dlt.table(
    name="caregapreport",
    comment="Bronze streaming table ingesting raw care gap CSV files via Auto Loader"
)
def caregapreport():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/claimspan/source/caregaptemp/")
    )