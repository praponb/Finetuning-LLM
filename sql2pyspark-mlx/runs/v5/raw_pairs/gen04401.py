from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2024-02-29")
        .select("firmware", "id")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
    )
)
