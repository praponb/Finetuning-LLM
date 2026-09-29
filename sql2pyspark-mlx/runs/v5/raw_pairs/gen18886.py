from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("pressure_max_2h").desc())

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("status_bits").between(6, 10))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.greatest("pressure", "temp_c").alias("greatest_pressure_temp_c"),
    )
)

filtered = (
    valid
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "pressure",
        "greatest_pressure_temp_c",
        F.round((F.col("greatest_pressure_temp_c") * 100), 2).alias("greatest_pressure_temp_c_scaled"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("device_code") == "DV-04")
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.max("pressure").over(w).alias("pressure_max_2h"),
        F.max("greatest_pressure_temp_c_scaled").over(w2).alias("greatest_pressure_temp_c_scaled_max_30m"),
    )
)

metrics = (
    enriched
    .withColumn("pressure_max_2h_count_15m", F.count("pressure_max_2h").over(w_recent))
    .withColumn("pressure_count_2h", F.count("pressure").over(w))
    .withColumn("pressure_max_2h_rank", F.rank().over(w4))
)

valid_device_readings = (
    metrics
    .filter(F.col("greatest_pressure_temp_c_scaled_max_30m") == 70)
    .select(
        "id",
        "reading_ts",
        "pressure",
        "greatest_pressure_temp_c",
        "greatest_pressure_temp_c_scaled",
        "pressure_max_2h",
        "pressure_max_2h_count_15m",
        "pressure_max_2h_rank",
        F.floor("pressure_max_2h_rank").alias("pressure_max_2h_rank_whole"),
    )
)

result = (
    valid_device_readings
    .select(
        "id",
        "reading_ts",
        "greatest_pressure_temp_c",
        "greatest_pressure_temp_c_scaled",
        "pressure_max_2h",
        "pressure_max_2h_count_15m",
        "pressure_max_2h_rank",
        "pressure_max_2h_rank_whole",
    )
    .orderBy(F.col("greatest_pressure_temp_c").desc())
)
