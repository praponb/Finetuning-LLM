from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.trim("firmware").alias("clean_firmware"),
    )
)

result = (
    filtered
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "1.0.3")
        .select("firmware", "id")
    )
)
