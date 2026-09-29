from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("device_code", "id")
    .exceptAll(
        df_device_readings
        .filter(F.col("device_code") == "DV-02")
        .select("device_code", "id")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
    )
    .orderBy(F.col("firmware").desc(), F.col("temp_c").desc())
)
