from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") < 7)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.lower("firmware").alias("firmware_lower"),
    )
)

result = (
    cleaned
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("status_bits") >= 5)
        .select("id")
    )
)
