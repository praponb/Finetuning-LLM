from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("humidity") >= 71, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("humidity_tier").desc_nulls_last())
)
