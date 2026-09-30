from pyspark.sql import functions as F

result = (
    df_device_readings
    .rollup("device_code", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("humidity_per_dv_04"),
        F.count("*").alias("event_count"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
    )
    .filter(F.col("humidity_stddev") < 0)
    .orderBy("humidity_stddev")
)
