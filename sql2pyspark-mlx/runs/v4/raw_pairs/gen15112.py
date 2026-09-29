from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("humidity") >= 34)
    .select(
        "*",
        F.when(F.col("device_code").isin(["DV-04", "DV-03"]), "Core").otherwise("Other").alias("device_code_group"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.percentile_approx("humidity", 0.95).alias("humidity_p95"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("status_bits_per_dv_03"),
    )
    .filter(F.col("humidity_p95") >= 0)
)
