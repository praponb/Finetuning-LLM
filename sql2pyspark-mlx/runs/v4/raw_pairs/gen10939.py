from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c") <= 32)
        .select("id", "device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
    )
    .orderBy(F.col("device_code").desc(), "reading_ts")
)
