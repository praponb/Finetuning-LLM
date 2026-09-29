from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms_adjusted").desc_nulls_last())
w2 = Window.orderBy(F.col("latency_ms_filled").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

filtered = (
    lines
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select("latency_ms", "attr_value", F.substring("k", 1, 4).alias("k_prefix"))
)

valid = (
    filtered
    .filter(F.col("latency_ms") < 1202)
    .select("*", F.round((F.col("latency_ms") * 1.07), 2).alias("latency_ms_adjusted"))
)

cleaned = (
    valid
    .filter(F.col("latency_ms_adjusted").between(67, 76))
    .select(
        "*",
        F.round("latency_ms_adjusted", 0).alias("latency_ms_adjusted_rounded"),
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("latency_ms_filled") <= 41)
    .select(
        "latency_ms",
        "attr_value",
        "k_prefix",
        F.cume_dist().over(w).alias("latency_ms_adjusted_cume_dist"),
        F.dense_rank().over(w2).alias("latency_ms_filled_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("latency_ms") >= 2437, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms_filled_dense_rank", 0.9).alias("latency_ms_filled_dense_rank_p90"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count("*").alias("num_rows"),
        F.max("latency_ms").alias("max_latency_ms"),
    )
    .filter(F.col("latency_ms_p95") >= 0)
)
