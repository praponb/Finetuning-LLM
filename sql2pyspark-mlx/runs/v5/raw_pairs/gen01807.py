from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("firmware", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("temp_c") <= 37)
        .select("firmware", "device_code")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("device_code", "temp_c")
    .orderBy("temp_c")
)
