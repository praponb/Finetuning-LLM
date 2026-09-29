from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy("status_code_high")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.orderBy(F.col("is_flagged").desc_nulls_last())
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.orderBy(F.col("days_since_log_ts_rounded").desc())
w8 = Window.orderBy("days_since_log_ts_count_2h_count_30m")
w9 = Window.orderBy(F.col("greatest_is_flagged_latency_ms").desc())
w10 = Window.orderBy(F.col("days_since_log_ts_rounded").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("log_ts") >= "2025-08-25")
    .select(
        "log_ts",
        "latency_ms",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    unpacked
    .filter(F.col("latency_ms") == 682)
    .select(
        "*",
        F.ceil("latency_ms").alias("latency_ms_whole"),
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

valid = (
    cleaned
    .filter(F.col("is_flagged") > 0)
    .select(
        "log_ts",
        "is_flagged",
        "status_code_high",
        "days_since_log_ts",
        F.greatest("is_flagged", "latency_ms").alias("greatest_is_flagged_latency_ms"),
        F.round("days_since_log_ts", 2).alias("days_since_log_ts_rounded"),
    )
)

with_windows = (
    valid
    .filter(F.col("is_flagged") >= 0)
    .select(
        "log_ts",
        "is_flagged",
        "greatest_is_flagged_latency_ms",
        "days_since_log_ts_rounded",
        F.max("status_code_high").over(w_recent).alias("status_code_high_max_2h"),
        F.count("days_since_log_ts").over(w2).alias("days_since_log_ts_count_15m"),
        F.dense_rank().over(w3).alias("status_code_high_dense_rank"),
        F.count("days_since_log_ts").over(w_recent).alias("days_since_log_ts_count_2h"),
    )
)

enriched = (
    with_windows
    .select(
        "is_flagged",
        "greatest_is_flagged_latency_ms",
        "days_since_log_ts_rounded",
        "status_code_high_max_2h",
        "days_since_log_ts_count_15m",
        F.count("days_since_log_ts_count_2h").over(w4).alias("days_since_log_ts_count_2h_count_30m"),
        F.sum("days_since_log_ts_rounded").over(w2).alias("days_since_log_ts_rounded_sum_15m"),
        F.cume_dist().over(w5).alias("is_flagged_cume_dist"),
        F.avg("is_flagged").over(w6).alias("is_flagged_avg_1h"),
    )
)

metrics = (
    enriched
    .withColumn("days_since_log_ts_rounded_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("days_since_log_ts_count_2h_count_30m_percent_rank", F.percent_rank().over(w8))
    .withColumn("greatest_is_flagged_latency_ms_cume_dist", F.cume_dist().over(w9))
    .withColumn("days_since_log_ts_rounded_dense_rank", F.dense_rank().over(w10))
)

result = (
    metrics
    .select(
        "greatest_is_flagged_latency_ms",
        "days_since_log_ts_rounded",
        "status_code_high_max_2h",
        "days_since_log_ts_count_15m",
        "days_since_log_ts_count_2h_count_30m",
        "days_since_log_ts_rounded_sum_15m",
        "is_flagged_cume_dist",
        "is_flagged_avg_1h",
        "days_since_log_ts_rounded_percent_rank",
    )
    .orderBy(F.col("status_code_high_max_2h").desc_nulls_first())
)
