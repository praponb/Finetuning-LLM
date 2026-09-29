from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "key").orderBy("latency_ms")
w2 = Window.partitionBy("service")
w3 = Window.orderBy(F.col("latency_ms_percent_rank").desc_nulls_first())
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("key")
w_running = Window.partitionBy("host", "key").orderBy(F.col("latency_ms").desc())
w_part = Window.orderBy(F.col("latency_ms_percent_rank").asc_nulls_last())
w8 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

with_windows = (
    exploded
    .filter(F.col("level").isin(["DEBUG", "INFO", "ERROR"]))
    .select(
        "*",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("status_code") > 336)
    .withColumn("status_code_minus_latency_ms", (F.col("status_code") - F.col("latency_ms")))
)

with_windows_app_logs = (
    cleaned
    .select(
        "host",
        "latency_ms",
        "key",
        "value",
        "latency_ms_percent_rank",
        "status_code_share",
        F.cume_dist().over(w3).alias("latency_ms_percent_rank_cume_dist"),
    )
)

scored = (
    with_windows_app_logs
    .select(
        "*",
        F.max("latency_ms_percent_rank").over(w4).alias("host_max_latency_ms_percent_rank"),
        F.sum("latency_ms_percent_rank").over(w5).alias("key_sum_latency_ms_percent_rank"),
        F.rank().over(w_running).alias("latency_ms_rank"),
        F.percent_rank().over(w_part).alias("latency_ms_percent_rank_percent_rank"),
        F.dense_rank().over(w8).alias("latency_ms_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("key").isNotNull() & (F.col("latency_ms_percent_rank_percent_rank") <= 0.31))
    .select(
        "*",
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
        (F.col("latency_ms_percent_rank_percent_rank") + F.col("latency_ms_percent_rank")).alias("latency_ms_percent_rank_percent_rank_plus_latency_ms_percent_rank"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

result = (
    valid
    .select(
        "key",
        "latency_ms_rank",
        "latency_ms_percent_rank_percent_rank",
        "latency_ms_filled",
        "latency_ms_percent_rank_percent_rank_plus_latency_ms_percent_rank",
        F.count("host_max_latency_ms_percent_rank").over(w4).alias("host_count_host_max_latency_ms_percent_rank"),
    )
)
