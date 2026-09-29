from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp")) & F.col("humidity").isNull())
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        F.greatest("status_bits", "pressure").alias("greatest_status_bits_pressure"),
    )
)

cleaned = (
    valid
    .filter(F.col("greatest_status_bits_pressure").between(57, 64))
    .select(
        "id",
        "humidity",
        "status_bits",
        "greatest_status_bits_pressure",
        (F.col("humidity") / F.nullif("greatest_status_bits_pressure", F.lit(0))).alias("humidity_per_greatest_status_bits_pressure"),
        F.greatest("status_bits", "humidity").alias("greatest_status_bits_humidity"),
    )
)

valid_device_readings = (
    cleaned
    .filter(F.col("humidity_per_greatest_status_bits_pressure").isNull())
    .select(
        "*",
        F.floor("humidity").alias("humidity_whole"),
        (F.col("status_bits") + F.col("greatest_status_bits_humidity")).alias("status_bits_plus_greatest_status_bits_humidity"),
    )
)

base = (
    valid_device_readings
    .filter(F.col("greatest_status_bits_humidity").between(66, 71))
    .select(
        "id",
        "greatest_status_bits_humidity",
        "humidity_whole",
        "status_bits_plus_greatest_status_bits_humidity",
        F.ceil("humidity_per_greatest_status_bits_pressure").alias("humidity_per_greatest_status_bits_pressure_whole"),
    )
)

filtered = (
    base
    .filter(F.col("status_bits_plus_greatest_status_bits_humidity").isNull())
    .select(
        "id",
        "humidity_whole",
        "status_bits_plus_greatest_status_bits_humidity",
        "humidity_per_greatest_status_bits_pressure_whole",
        F.round("humidity_whole", 0).alias("humidity_whole_rounded"),
        (F.col("humidity_whole") - F.col("greatest_status_bits_humidity")).alias("humidity_whole_minus_greatest_status_bits_humidity"),
    )
)

cleaned_device_readings = (
    filtered
    .filter(F.col("status_bits_plus_greatest_status_bits_humidity") > 32)
    .select(
        "id",
        "humidity_whole",
        "status_bits_plus_greatest_status_bits_humidity",
        "humidity_per_greatest_status_bits_pressure_whole",
        "humidity_whole_rounded",
        F.when(F.col("status_bits_plus_greatest_status_bits_humidity") >= 89, "High").when(F.col("status_bits_plus_greatest_status_bits_humidity") >= 27, "Medium").otherwise("Low").alias("status_bits_plus_greatest_status_bits_humidity_level"),
    )
)

base_device_readings = (
    cleaned_device_readings
    .filter(F.col("humidity_whole_rounded") >= 48)
    .select(
        "id",
        "humidity_whole",
        "status_bits_plus_greatest_status_bits_humidity",
        "humidity_whole_rounded",
        "status_bits_plus_greatest_status_bits_humidity_level",
        (F.col("humidity_whole_rounded") - F.col("humidity_whole")).alias("humidity_whole_rounded_minus_humidity_whole"),
    )
)

result = (
    base_device_readings
    .select(
        "id",
        "humidity_whole",
        "status_bits_plus_greatest_status_bits_humidity",
        "humidity_whole_rounded",
        "humidity_whole_rounded_minus_humidity_whole",
    )
)
