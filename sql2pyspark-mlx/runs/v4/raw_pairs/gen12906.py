from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.concat("device_code", F.lit(" - "), "firmware").alias("device_code_firmware"),
        F.lower("firmware").alias("firmware_lower"),
    )
)

cleaned_device_readings = (
    cleaned
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "device_code_firmware",
        F.floor("humidity").alias("humidity_whole"),
    )
)

filtered = (
    cleaned_device_readings
    .filter(F.col("humidity_whole") >= 44)
    .withColumn("humidity_whole_band", F.when(F.col("humidity_whole") >= 80, "High").when(F.col("humidity_whole") >= 34, "Medium").otherwise("Low"))
)

result = (
    filtered
    .select("id", "device_code_firmware", "humidity_whole", "humidity_whole_band")
)
