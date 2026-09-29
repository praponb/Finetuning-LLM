from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.count("*").alias("row_count"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .filter(F.col("dv_02_rows") >= 1)
)
