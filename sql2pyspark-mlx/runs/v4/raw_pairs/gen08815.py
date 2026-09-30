from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["device_code", "hour_window"], ["device_code"]],
        "device_code",
        "hour_window",
    )
    .agg(
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("status_bits"))).alias("dv_02_max_status_bits"),
        F.max("reading_ts").alias("last_seen"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events_2"),
        F.grouping("device_code").alias("grouping_id"),
    )
    .filter(F.col("humidity_stddev") >= 0)
)
