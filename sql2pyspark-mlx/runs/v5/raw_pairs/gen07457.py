from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key", "level")
w_recent = Window.partitionBy("service")
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level", "service")
w7 = Window.orderBy(F.col("latency_ms_cume_dist").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    exploded
    .filter(F.col("service").isin(["checkout", "billing"]))
    .select(
        "*",
        F.sum("latency_ms").over(w_running).alias("attr_key_sum_latency_ms"),
        F.corr("status_code", "latency_ms").over(w_recent).alias("corr_status_code_latency_ms"),
        F.corr("latency_ms", "status_code").over(w_recent).alias("corr_latency_ms_status_code"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist_2"),
    )
)

base = (
    with_windows
    .filter(F.col("latency_ms_cume_dist") <= 0.72)
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

with_windows_app_logs = (
    base
    .select(
        "service",
        "level",
        "v",
        "latency_ms_cume_dist",
        F.corr("latency_ms", "status_code").over(w5).alias("corr_latency_ms_status_code_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4).alias("latency_ms_share"),
    )
)

base_app_logs = (
    with_windows_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "service",
        "level",
        "v",
        "latency_ms_cume_dist",
        "corr_latency_ms_status_code_2",
        "latency_ms_share",
        F.round("latency_ms_cume_dist", 2).alias("latency_ms_cume_dist_rounded"),
    )
)

with_windows_app_logs_2 = (
    base_app_logs
    .select(
        "service",
        "latency_ms_cume_dist",
        "corr_latency_ms_status_code_2",
        "latency_ms_share",
        "latency_ms_cume_dist_rounded",
        F.percent_rank().over(w7).alias("latency_ms_cume_dist_percent_rank"),
    )
)

valid = (
    with_windows_app_logs_2
    .filter(F.col("latency_ms_cume_dist_rounded").between(23, 65))
    .select(
        "*",
        F.col("latency_ms_cume_dist_percent_rank").cast("int").alias("latency_ms_cume_dist_percent_rank_int"),
    )
)

result = (
    valid
    .select(
        "latency_ms_cume_dist",
        "latency_ms_share",
        "latency_ms_cume_dist_rounded",
        "latency_ms_cume_dist_percent_rank",
        "latency_ms_cume_dist_percent_rank_int",
    )
    .orderBy("latency_ms_cume_dist_percent_rank")
)
