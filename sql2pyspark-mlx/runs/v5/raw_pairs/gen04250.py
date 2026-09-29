from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.partitionBy("key")
w4 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

metrics = (
    flattened
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("latency_ms_avg_2h", F.avg("latency_ms").over(w2))
)

cleaned = (
    metrics
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 13:00:00")))
    .select(
        "log_ts",
        "key",
        "latency_ms_avg_2h",
        (F.col("latency_ms_percent_rank") * F.col("latency_ms")).alias("latency_ms_percent_rank_times_latency_ms"),
    )
)

enriched = (
    cleaned
    .select(
        "log_ts",
        "key",
        "latency_ms_avg_2h",
        "latency_ms_percent_rank_times_latency_ms",
        F.avg("latency_ms_percent_rank_times_latency_ms").over(w_ordered).alias("key_avg_latency_ms_percent_rank_times_latency_ms"),
        F.avg("latency_ms_avg_2h").over(w_ordered).alias("key_avg_latency_ms_avg_2h"),
        F.avg("latency_ms_avg_2h").over(w4).alias("latency_ms_avg_2h_avg_1h"),
    )
)

valid = (
    enriched
    .filter(F.col("key").isNotNull() & F.hour("log_ts").between(9, 13))
    .select(
        "log_ts",
        "latency_ms_avg_2h",
        "latency_ms_percent_rank_times_latency_ms",
        "key_avg_latency_ms_avg_2h",
        "latency_ms_avg_2h_avg_1h",
        F.coalesce("latency_ms_avg_2h", F.lit(0)).alias("latency_ms_avg_2h_filled"),
        F.greatest("latency_ms_avg_2h", "latency_ms_percent_rank_times_latency_ms").alias("greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms"),
    )
)

scored = (
    valid
    .filter(F.col("greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms") >= 67)
    .select(
        "*",
        F.sum("greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms").over(w5).alias("greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m"),
    )
)

result = (
    scored
    .select(
        "latency_ms_avg_2h_avg_1h",
        "latency_ms_avg_2h_filled",
        "greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms",
        "greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m",
    )
    .orderBy(
        "greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m",
        F.col("latency_ms_avg_2h_avg_1h").desc(),
    )
)
