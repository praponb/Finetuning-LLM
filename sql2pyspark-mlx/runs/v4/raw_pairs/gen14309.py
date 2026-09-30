from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("hour_window", F.date_trunc("hour", "reading_ts"))
    .rollup("device_code", "hour_window")
    .agg(
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("status_bits")).otherwise(0)).alias("dv_03_status_bits"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.max("reading_ts").alias("last_seen"),
        F.grouping("device_code").alias("grouping_id"),
    )
)
