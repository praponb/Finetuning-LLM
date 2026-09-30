from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("firmware", "id")
    .intersect(
        df_device_readings
        .filter(F.hour("reading_ts").between(9, 16))
        .select("firmware", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
    )
    .filter(F.col("dv_04_count") > 1)
)
