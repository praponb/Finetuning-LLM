from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 09:00:00")))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.substring("firmware", 1, 4).alias("firmware_prefix"),
    )
)

result = (
    cleaned
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") != "DV-02")
        .select("id")
    )
)
