from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_hist = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("firmware").isin(["1.0.3", "2.0.1", "1.1.0"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.trim("device_code").alias("clean_device_code"),
    )
)

base = (
    cleaned
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 07:00:00"))) & F.col("humidity").between(59, 65))
    .select(
        "*",
        F.when(F.col("humidity") >= 82, "High").when(F.col("humidity") >= 41, "Medium").otherwise("Low").alias("humidity_band"),
    )
)

matched_keys = (
    base
    .select("id")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c") > 39)
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("humidity_count_15m", F.count("humidity").over(w))
    .withColumn("humidity_count_1h", F.count("humidity").over(w_hist))
    .withColumn("temp_c_sum_2h", F.sum("temp_c").over(w3))
    .withColumn("pressure_max_30m", F.max("pressure").over(w4))
    .withColumn("temp_c_max_1h", F.max("temp_c").over(w_hist))
)

summary = (
    metrics
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .withColumn("pressure_max_30m_tier", F.when(F.col("pressure_max_30m") >= 1017, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["hour_window", "pressure_max_30m_tier"], ["hour_window"], []],
        "hour_window",
        "pressure_max_30m_tier",
    )
    .agg(
        F.percentile_approx("pressure_max_30m", [0.25, 0.5, 0.75]).alias("pressure_max_30m_quartiles"),
        F.percentile_approx("humidity_count_15m", 0.5).alias("humidity_count_15m_p50"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.min("reading_ts").alias("started_at"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_pressure") > 0)
)

result = (
    summary
    .select(
        "hour_window",
        "avg_pressure",
        "pressure_p90",
        "humidity_quartiles",
        "started_at",
        "grouping_id",
    )
)
