from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("id", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("humidity") <= 75)
        .select("id", "firmware")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 80, "LARGE").otherwise("SMALL"))
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["humidity_tier", "activity_day", "hour_window"], ["humidity_tier", "activity_day"], ["humidity_tier"], []],
        "humidity_tier",
        "activity_day",
        "hour_window",
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.count("*").alias("cnt"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.max("reading_ts").alias("ended_at"),
        F.corr("pressure", "temp_c").alias("corr_pressure_temp_c"),
    )
)
