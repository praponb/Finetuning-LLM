from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy("latency_ms")
w4 = Window.partitionBy("attr_key")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.orderBy(F.col("latency_ms_percent_rank").desc_nulls_last())
w9 = Window.partitionBy("service").orderBy(F.col("attr_key_count_latency_ms_count_30m").desc_nulls_last())
w10 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

metrics = (
    lines
    .filter(F.col("service").isin(["search", "checkout"]))
    .select(
        "*",
        F.count("latency_ms").over(w).alias("latency_ms_count_1h"),
        F.sum("latency_ms").over(w2).alias("latency_ms_sum_15m"),
        F.percent_rank().over(w3).alias("latency_ms_percent_rank"),
        F.count("latency_ms").over(w4).alias("attr_key_count_latency_ms"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "id",
        "service",
        "log_ts",
        "value",
        "latency_ms_count_1h",
        "latency_ms_sum_15m",
        "latency_ms_percent_rank",
        "attr_key_count_latency_ms",
        F.count("attr_key_count_latency_ms").over(w5).alias("attr_key_count_latency_ms_count_30m"),
        F.avg("latency_ms_percent_rank").over(w_part).alias("latency_ms_percent_rank_avg_1h"),
        F.avg("attr_key_count_latency_ms").over(w_running).alias("attr_key_count_latency_ms_avg_1h"),
        F.cume_dist().over(w_grp).alias("latency_ms_percent_rank_cume_dist"),
    )
)

valid = (
    metrics_app_logs
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("latency_ms_percent_rank_avg_1h").between(0, 1))
    .select("*", F.round("latency_ms_sum_15m", 1).alias("latency_ms_sum_15m_rounded"))
)

with_windows = (
    valid
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms_sum_15m",
        "latency_ms_sum_15m_rounded",
        F.rank().over(w9).alias("attr_key_count_latency_ms_count_30m_rank"),
        F.min("attr_key_count_latency_ms").over(w10).alias("service_min_attr_key_count_latency_ms"),
    )
)

result = (
    with_windows
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .withColumn("latency_ms_sum_15m_tier", F.when(F.col("latency_ms_sum_15m") >= 11376, "LARGE").otherwise("SMALL"))
    .withColumn("service_min_attr_key_count_latency_ms_tier", F.when(F.col("service_min_attr_key_count_latency_ms") >= 16, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["hour_window", "latency_ms_sum_15m_tier", "service_min_attr_key_count_latency_ms_tier"], ["hour_window", "latency_ms_sum_15m_tier"], ["hour_window"]],
        "hour_window",
        "latency_ms_sum_15m_tier",
        "service_min_attr_key_count_latency_ms_tier",
    )
    .agg(
        F.round(F.avg("latency_ms_sum_15m_rounded"), 2).alias("avg_latency_ms_sum_15m_rounded"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.count("*").alias("cnt"),
        F.round(F.sum("latency_ms_sum_15m") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("latency_ms_sum_15m_per_billing"),
        F.percentile_approx("latency_ms_sum_15m_rounded", 0.9).alias("latency_ms_sum_15m_rounded_p90"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("checkout_rows") >= 1)
    .orderBy("checkout_rows")
)
