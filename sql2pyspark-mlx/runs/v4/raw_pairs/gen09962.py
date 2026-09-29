from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("service")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("level", "service")
w5 = Window.orderBy(F.col("service_stddev_samp_latency_ms_adjusted").desc_nulls_first())
w6 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.partitionBy("key")
w9 = Window.partitionBy("key", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w10 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w11 = Window.partitionBy("level", "host")
w12 = Window.partitionBy("key").orderBy(F.col("service_max_latency_ms_share").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("log_ts") < "2024-08-01")
    .withColumn("latency_ms_adjusted", F.round((F.col("latency_ms") * 0.9), 2))
    .withColumn("key_filled", F.coalesce("key", F.lit("Unknown")))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

scored = (
    valid
    .withColumn("latency_ms_avg_15m", F.avg("latency_ms").over(w))
    .withColumn("service_stddev_samp_latency_ms_adjusted", F.stddev_samp("latency_ms_adjusted").over(w_part))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("level_stddev_samp_latency_ms_adjusted", F.stddev_samp("latency_ms_adjusted").over(w4))
    .withColumn("service_max_latency_ms", F.max("latency_ms").over(w_part))
)

metrics = (
    scored
    .withColumn("service_max_latency_ms_share", F.round((F.col("service_max_latency_ms") / F.sum("service_max_latency_ms").over(w_part)), 4))
    .withColumn("service_stddev_samp_latency_ms_adjusted_dense_rank", F.dense_rank().over(w5))
)

filtered = (
    metrics
    .filter((F.col("log_ts") > F.lit("2025-06-01 18:00:00").cast("timestamp")) & F.col("latency_ms").between(1262, 1811))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "v",
        "latency_ms_adjusted",
        "key_filled",
        "resolved_key",
        "latency_ms_avg_15m",
        "service_stddev_samp_latency_ms_adjusted",
        "latency_ms_share",
        "level_stddev_samp_latency_ms_adjusted",
        "service_max_latency_ms_share",
        "service_stddev_samp_latency_ms_adjusted_dense_rank",
        F.coalesce("service_stddev_samp_latency_ms_adjusted_dense_rank", F.lit(0)).alias("service_stddev_samp_latency_ms_adjusted_dense_rank_filled"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key_2"),
    )
)

enriched = (
    filtered
    .withColumn("service_max_latency_ms_share_count_15m", F.count("service_max_latency_ms_share").over(w6))
    .withColumn("latency_ms_avg_15m_avg_1h", F.avg("latency_ms_avg_15m").over(w7))
    .withColumn("key_min_service_max_latency_ms_share", F.min("service_max_latency_ms_share").over(w8))
)

metrics_app_logs = (
    enriched
    .withColumn("key_min_service_max_latency_ms_share_avg_30m", F.avg("key_min_service_max_latency_ms_share").over(w9))
    .withColumn("latency_ms_avg_15m_avg_1h_count_1h", F.count("latency_ms_avg_15m_avg_1h").over(w10))
)

result = (
    metrics_app_logs
    .withColumn("level_min_latency_ms_avg_15m_avg_1h", F.min("latency_ms_avg_15m_avg_1h").over(w11))
    .withColumn("service_max_latency_ms_share_cume_dist", F.cume_dist().over(w12))
)
