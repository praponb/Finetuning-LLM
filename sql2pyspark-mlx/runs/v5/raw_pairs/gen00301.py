from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("service", "level")
w7 = Window.partitionBy("service")
w8 = Window.partitionBy("key")
w9 = Window.orderBy(F.col("latency_ms_avg_15m_minus_latency_ms").asc_nulls_last())
w10 = Window.partitionBy("host").orderBy(F.col("latency_ms_max_15m_share").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.max("latency_ms").over(w).alias("latency_ms_max_15m"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_30m"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_15m"),
        F.count("latency_ms").over(w4).alias("latency_ms_count_30m"),
        F.avg("latency_ms").over(w5).alias("latency_ms_avg_2h"),
    )
)

filtered = (
    scored
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("service").isin(["search", "checkout", "billing"]))
    .select(
        "*",
        (F.col("latency_ms_avg_15m") - F.col("latency_ms")).alias("latency_ms_avg_15m_minus_latency_ms"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

with_windows = (
    filtered
    .select(
        "service",
        "host",
        "log_ts",
        "key",
        "value",
        "latency_ms_max_15m",
        "latency_ms_avg_30m",
        "latency_ms_avg_2h",
        "latency_ms_avg_15m_minus_latency_ms",
        "resolved_key",
        F.round((F.col("latency_ms_max_15m") / F.sum("latency_ms_max_15m").over(w6)), 4).alias("latency_ms_max_15m_share"),
    )
)

metrics = (
    with_windows
    .filter(F.col("latency_ms_avg_30m") >= 708)
    .select(
        "*",
        F.sum("latency_ms_max_15m").over(w7).alias("service_sum_latency_ms_max_15m"),
        F.round((F.col("latency_ms_avg_2h") / F.sum("latency_ms_avg_2h").over(w8)), 4).alias("latency_ms_avg_2h_share"),
        F.count("latency_ms_max_15m_share").over(w8).alias("key_count_latency_ms_max_15m_share"),
        F.percent_rank().over(w9).alias("latency_ms_avg_15m_minus_latency_ms_percent_rank"),
        F.rank().over(w10).alias("latency_ms_max_15m_share_rank"),
    )
)

base = (
    metrics
    .filter(F.col("latency_ms_avg_2h_share").isNotNull())
    .withColumn("latency_ms_avg_2h_per_latency_ms_max_15m", F.round((F.col("latency_ms_avg_2h") / F.col("latency_ms_max_15m")), 2))
)

result = (
    base
    .groupBy("service")
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.percentile_approx("service_sum_latency_ms_max_15m", [0.25, 0.5, 0.75]).alias("service_sum_latency_ms_max_15m_quartiles"),
    )
)
