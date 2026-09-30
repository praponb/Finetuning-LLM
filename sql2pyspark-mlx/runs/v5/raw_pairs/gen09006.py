from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    parsed
    .filter(F.col("status_code_high") == 1)
    .select(
        "*",
        F.when(F.col("k") == "trace", 1).otherwise(0).alias("is_trace"),
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

base = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("is_trace") + F.col("days_since_log_ts")).alias("is_trace_plus_days_since_log_ts"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

base_app_logs = (
    base
    .filter(F.col("k").isNotNull() & (F.col("is_trace") <= 65))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "is_flagged",
        "status_code_high",
        "is_trace",
        "days_since_log_ts",
        "is_trace_plus_days_since_log_ts",
        "k_filled",
        (F.col("latency_ms") + F.col("is_trace")).alias("latency_ms_plus_is_trace"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k_2"),
    )
)

valid = (
    base_app_logs
    .filter(F.col("log_ts") > F.lit("2025-06-01 11:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "is_flagged",
        "status_code_high",
        "days_since_log_ts",
        "is_trace_plus_days_since_log_ts",
        "latency_ms_plus_is_trace",
        "resolved_k_2",
        F.least("days_since_log_ts", "is_trace").alias("least_days_since_log_ts_is_trace"),
        F.round("status_code_high", 2).alias("status_code_high_rounded"),
    )
)

scored = (
    valid
    .withColumn("status_code_high_rank", F.rank().over(w))
    .withColumn("is_flagged_avg_1h", F.avg("is_flagged").over(w_hist))
    .withColumn("days_since_log_ts_sum_1h", F.sum("days_since_log_ts").over(w_hist))
    .withColumn("least_days_since_log_ts_is_trace_sum_1h", F.sum("least_days_since_log_ts_is_trace").over(w_hist))
    .withColumn("least_days_since_log_ts_is_trace_count_2h", F.count("least_days_since_log_ts_is_trace").over(w3))
)

result = (
    scored
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("status_code_high_rounded", 0.95).alias("status_code_high_rounded_p95"),
        F.max("status_code_high_rank").alias("max_status_code_high_rank"),
        F.sum("status_code_high_rounded").alias("total_status_code_high_rounded"),
    )
)
