from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.min("reading_ts").alias("first_seen"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("cnt") < 1)
    .orderBy(F.col("corr_pressure_status_bits").desc())
)
