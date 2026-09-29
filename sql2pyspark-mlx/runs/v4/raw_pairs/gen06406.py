from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "2.0.1")
        .select("firmware")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
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
    )
    .orderBy(F.col("status_bits").desc())
)
