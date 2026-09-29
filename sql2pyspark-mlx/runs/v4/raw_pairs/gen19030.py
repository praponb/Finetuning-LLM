from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("humidity_int").desc_nulls_last())
w3 = Window.orderBy(F.col("humidity_avg_30m").desc())
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("device_code") == "DV-03")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") + F.col("temp_c")).alias("status_bits_plus_temp_c"),
        F.round("status_bits", 2).alias("status_bits_rounded"),
    )
)

cleaned = (
    base
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_plus_temp_c",
        F.ceil("status_bits").alias("status_bits_whole"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_30m"),
        F.count("status_bits_whole").over(w).alias("status_bits_whole_count_30m"),
    )
)

matched_keys = (
    scored
    .select("id", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(7, 15))
        .select("id", "firmware")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select("*", F.col("humidity").cast("int").alias("humidity_int"))
)

base_device_readings = (
    valid
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 10:00:00").cast("timestamp"))
    .select("*", F.floor("status_bits").alias("status_bits_whole_2"))
)

with_windows = (
    base_device_readings
    .withColumn("humidity_int_rank", F.rank().over(w2))
    .withColumn("humidity_avg_30m_percent_rank", F.percent_rank().over(w3))
    .withColumn("status_bits_sum_1h", F.sum("status_bits").over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_plus_temp_c",
        "status_bits_whole",
        "status_bits_whole_count_30m",
        "status_bits_whole_2",
    )
)
