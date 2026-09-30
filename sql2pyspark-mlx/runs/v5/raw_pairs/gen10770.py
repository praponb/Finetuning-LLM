from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.max("humidity").alias("max_humidity"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity"))).alias("dv_02_total_humidity"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.max("reading_ts").alias("ended_at"),
    )
    .orderBy(F.col("device_code").asc_nulls_last())
)
