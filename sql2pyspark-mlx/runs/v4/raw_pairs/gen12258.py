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
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.grouping("device_code").alias("grouping_id"),
    )
    .orderBy("hour_window")
)
