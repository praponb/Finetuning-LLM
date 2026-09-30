from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("pressure").isNull())
        .select("firmware", "device_code")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("device_code", "reading_ts", "temp_c", "humidity", "pressure", "firmware")
    .orderBy(F.col("temp_c").asc_nulls_first())
)
