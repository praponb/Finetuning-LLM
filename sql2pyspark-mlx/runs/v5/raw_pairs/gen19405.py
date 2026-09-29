from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("service").orderBy(F.col("latency_ms_times_latency_ms_rounded").asc_nulls_last())
w2 = Window.orderBy(F.col("latency_ms_rounded").desc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())
w_running = Window.partitionBy("host")
w5 = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

base = (
    lines
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .select(
        "*",
        F.lower("host").alias("host_lower"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

filtered = (
    base
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        "key_filled",
        F.round("latency_ms", 1).alias("latency_ms_rounded"),
        F.floor("latency_ms").alias("latency_ms_whole"),
    )
)

valid = (
    filtered
    .filter(F.col("latency_ms") <= 1814)
    .select(
        "*",
        (F.col("latency_ms") * F.col("latency_ms_rounded")).alias("latency_ms_times_latency_ms_rounded"),
        F.concat_ws("/", "host", "key").alias("host_key_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled_2"),
    )
)

with_windows = (
    valid
    .filter(F.col("latency_ms_whole") == 32)
    .select(
        "id",
        "service",
        "latency_ms",
        "latency_ms_whole",
        "latency_ms_times_latency_ms_rounded",
        "key_filled_2",
        F.percent_rank().over(w_grp).alias("latency_ms_times_latency_ms_rounded_percent_rank"),
        F.cume_dist().over(w2).alias("latency_ms_rounded_cume_dist"),
        F.rank().over(w3).alias("latency_ms_rank"),
        F.count("latency_ms_whole").over(w_running).alias("host_count_latency_ms_whole"),
        F.count("latency_ms_times_latency_ms_rounded").over(w5).alias("key_count_latency_ms_times_latency_ms_rounded"),
    )
)

result = (
    with_windows
    .select(
        "service",
        "latency_ms_times_latency_ms_rounded",
        "key_filled_2",
        "latency_ms_times_latency_ms_rounded_percent_rank",
        "latency_ms_rank",
        "key_count_latency_ms_times_latency_ms_rounded",
    )
    .orderBy(
        F.col("latency_ms_times_latency_ms_rounded_percent_rank").asc_nulls_first(),
        F.col("latency_ms_rank").desc(),
    )
)
