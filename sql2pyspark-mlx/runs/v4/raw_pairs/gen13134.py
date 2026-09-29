from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("host", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("k", "host")
w4 = Window.partitionBy("host", "k").orderBy(F.col("latency_ms").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01 11:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .withColumn("k_lower", F.lower("k"))
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
    .withColumn("k_filled", F.coalesce("k", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w))
    .withColumn("latency_ms_max_15m", F.max("latency_ms").over(w2))
)

filtered = (
    with_windows
    .filter(F.col("k").isNotNull())
    .withColumn("clean_host", F.trim("host"))
    .withColumn("latency_ms_max_15m_plus_latency_ms_filled", (F.col("latency_ms_max_15m") + F.col("latency_ms_filled")))
)

with_windows_app_logs = (
    filtered
    .select(
        "*",
        F.avg("latency_ms_max_15m_plus_latency_ms_filled").over(w_part).alias("k_avg_latency_ms_max_15m_plus_latency_ms_filled"),
        F.percent_rank().over(w4).alias("latency_ms_percent_rank"),
    )
)

totals = (
    with_windows_app_logs
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("latency_ms_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("latency_ms_dense_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "trace", F.col("latency_ms_max_15m"))).alias("trace_total_latency_ms_max_15m"),
        F.round(F.avg("latency_ms_max_15m"), 2).alias("avg_latency_ms_max_15m"),
    )
    .filter(F.col("trace_total_latency_ms_max_15m") < 0)
)

result = (
    totals
    .select("hour_window", "latency_ms_dense_rank_tier", "avg_latency_ms_max_15m")
    .orderBy(F.col("latency_ms_dense_rank_tier").asc_nulls_last())
)
