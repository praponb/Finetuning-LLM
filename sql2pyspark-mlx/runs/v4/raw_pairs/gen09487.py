from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.partitionBy("level", "service").orderBy(F.col("least_latency_ms_status_code").desc())
w5 = Window.partitionBy("service")
w_hist = Window.partitionBy("level")
w7 = Window.partitionBy("service", "level").orderBy(F.col("latency_ms_cume_dist").asc_nulls_last())
w8 = Window.orderBy(F.col("latency_ms_dense_rank").desc_nulls_last())
w9 = Window.partitionBy("level", "service").orderBy("latency_ms_cume_dist")
w10 = Window.partitionBy("service").orderBy(F.col("latency_ms_cume_dist_percent_rank").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("level") != "WARN")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_value",
        F.least("latency_ms", "status_code").alias("least_latency_ms_status_code"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w4).alias("least_latency_ms_status_code_cume_dist"),
        F.sum("latency_ms").over(w5).alias("service_sum_latency_ms"),
    )
)

valid = (
    metrics
    .filter(F.col("least_latency_ms_status_code") > 69)
    .withColumn("clean_service", F.trim("service"))
)

with_windows = (
    valid
    .select(
        "*",
        F.count("least_latency_ms_status_code_cume_dist").over(w_hist).alias("level_count_least_latency_ms_status_code_cume_dist"),
        F.percent_rank().over(w7).alias("latency_ms_cume_dist_percent_rank"),
        F.min("least_latency_ms_status_code_cume_dist").over(w_hist).alias("level_min_least_latency_ms_status_code_cume_dist"),
        F.percent_rank().over(w8).alias("latency_ms_dense_rank_percent_rank"),
        F.avg("least_latency_ms_status_code_cume_dist").over(w_hist).alias("level_avg_least_latency_ms_status_code_cume_dist"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.rank().over(w9).alias("latency_ms_cume_dist_rank"),
        F.rank().over(w10).alias("latency_ms_cume_dist_percent_rank_rank"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_value",
        "least_latency_ms_status_code",
        "latency_ms_percent_rank",
        "least_latency_ms_status_code_cume_dist",
        "clean_service",
    )
    .orderBy(F.col("latency_ms").desc_nulls_first())
)
