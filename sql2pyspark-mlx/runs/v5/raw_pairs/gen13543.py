from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("is_flagged").desc())
w_recent = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_grp = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy(F.col("status_code_per_latency_ms").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

unpacked = (
    exploded
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    unpacked
    .filter(F.col("status_code_high") == 2)
    .select(
        "*",
        F.rank().over(w_hist).alias("is_flagged_rank"),
        F.sum("latency_ms").over(w_recent).alias("latency_ms_sum_15m"),
        F.cume_dist().over(w_grp).alias("status_code_high_cume_dist"),
        F.count("status_code_high").over(w_recent).alias("status_code_high_count_15m"),
    )
)

valid = (
    with_windows
    .filter(F.col("log_ts") > F.lit("2025-06-01 14:00:00").cast("timestamp"))
    .withColumn("status_code_per_latency_ms", F.round((F.col("status_code") / F.col("latency_ms")), 2))
    .withColumn("status_code_high_cume_dist_times_is_flagged_rank", (F.col("status_code_high_cume_dist") * F.col("is_flagged_rank")))
)

result = (
    valid
    .select(
        "latency_ms",
        "v",
        "status_code_high",
        "is_flagged",
        "is_flagged_rank",
        "status_code_high_count_15m",
        "status_code_per_latency_ms",
        "status_code_high_cume_dist_times_is_flagged_rank",
        F.max("status_code_high_cume_dist").over(w_recent).alias("status_code_high_cume_dist_max_15m"),
        F.avg("status_code_per_latency_ms").over(w4).alias("status_code_per_latency_ms_avg_2h"),
        F.rank().over(w5).alias("status_code_per_latency_ms_rank"),
    )
)
