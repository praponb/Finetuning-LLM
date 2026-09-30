from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 63, "LARGE").otherwise("SMALL").alias("humidity_tier"),
        "device_code",
    )
    .agg(
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("status_bits"))).alias("dv_04_avg_status_bits"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.min("reading_ts").alias("started_at"),
    )
)
