from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-04-01")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

base = (
    valid
    .filter(F.col("status_bits") < 8)
    .withColumn("least_humidity_temp_c", F.least("humidity", "temp_c"))
)

remaining_keys = (
    base
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.col("pressure") <= 1025)
        .select("firmware", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_device_readings = (
    verified
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "pressure_filled",
        F.round("pressure", 1).alias("pressure_rounded"),
        F.round((F.col("pressure_filled") / F.col("humidity")), 2).alias("pressure_filled_per_humidity"),
    )
)

valid_device_readings_2 = (
    valid_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("humidity") > 36))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "pressure_filled",
        "pressure_rounded",
        "pressure_filled_per_humidity",
        F.round((F.col("temp_c") * 0.85), 2).alias("temp_c_scaled"),
    )
)

result = (
    valid_device_readings_2
    .select(
        "humidity",
        "firmware",
        "pressure_rounded",
        "pressure_filled_per_humidity",
        "temp_c_scaled",
    )
    .orderBy(F.col("firmware").desc_nulls_first())
)
