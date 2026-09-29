from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("host", "level").orderBy(F.col("level_min_latency_ms").desc())
w4 = Window.partitionBy("key", "service").orderBy(F.col("status_code_cume_dist").desc(), "id", "key")
w_part = Window.partitionBy("level").orderBy("event_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("service").orderBy("level_min_latency_ms_rank_share", "id", "key").rowsBetween(-2, -1)
w_recent = Window.partitionBy("host", "key").orderBy("running_sum_latency_ms", "id", "key").rowsBetween(-3, -1)
w9 = Window.orderBy("running_avg_latency_ms", "id", "key").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("key", "service").orderBy(F.col("status_code_cume_dist_avg_2_prior").desc_nulls_last(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.min("latency_ms").over(w2).alias("level_min_latency_ms"),
    )
)

with_windows_app_logs = (
    with_windows
    .filter(F.col("service").isin(["billing", "checkout"]))
    .withColumn("level_min_latency_ms_rank", F.rank().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

with_windows_app_logs_2 = (
    with_windows_app_logs
    .withColumn("running_count_latency_ms", F.count("latency_ms").over(w_part))
    .withColumn("running_sum_latency_ms", F.sum("latency_ms").over(w_part))
    .withColumn("running_avg_latency_ms", F.avg("latency_ms").over(w_part))
    .withColumn("running_max_latency_ms", F.max("latency_ms").over(w_part))
    .withColumn("level_min_latency_ms_rank_share", F.round((F.col("level_min_latency_ms_rank") / F.sum("level_min_latency_ms_rank").over(w6)), 4))
)

metrics = (
    with_windows_app_logs_2
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "attr_value",
        "status_code_cume_dist",
        "level_min_latency_ms_rank",
        "running_count_latency_ms",
        "running_max_latency_ms",
        F.avg("status_code_cume_dist").over(w7).alias("status_code_cume_dist_avg_2_prior"),
        F.avg("status_code_cume_dist").over(w_recent).alias("status_code_cume_dist_avg_3_prior"),
        F.avg("running_sum_latency_ms").over(w9).alias("running_sum_latency_ms_avg_3"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter(F.col("key").eqNullSafe(F.lit("version")))
    .select(
        "*",
        F.floor("running_count_latency_ms").alias("running_count_latency_ms_whole"),
        F.concat("service", F.lit("/"), "key").alias("service_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "key",
        "attr_value",
        "running_count_latency_ms",
        "running_max_latency_ms",
        "status_code_cume_dist_avg_2_prior",
        "status_code_cume_dist_avg_3_prior",
        "running_sum_latency_ms_avg_3",
    )
)
