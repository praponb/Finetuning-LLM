from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("service", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("service", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms").asc_nulls_last())
w7 = Window.partitionBy("service").orderBy(F.col("latency_ms_cume_dist").asc_nulls_last())
w8 = Window.partitionBy("service").orderBy(F.col("latency_ms_cume_dist_2").asc_nulls_first())
w9 = Window.orderBy(F.col("status_code_dense_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "*",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

enriched = (
    cleaned
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w))
    .withColumn("status_code_count_15m", F.count("status_code").over(w2))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w3))
)

enriched_app_logs = (
    enriched
    .withColumn("service_avg_status_code_dense_rank", F.avg("status_code_dense_rank").over(w4))
    .withColumn("latency_ms_cume_dist_count_1h", F.count("latency_ms_cume_dist").over(w5))
    .withColumn("latency_ms_cume_dist_2", F.cume_dist().over(w6))
    .withColumn("latency_ms_cume_dist_dense_rank", F.dense_rank().over(w7))
    .withColumn("service_min_days_since_log_ts", F.min("days_since_log_ts").over(w4))
)

with_windows = (
    enriched_app_logs
    .filter(F.col("latency_ms_cume_dist_2") <= 0.3)
    .select(
        "id",
        "log_ts",
        "status_code",
        "days_since_log_ts",
        "key_filled",
        "latency_ms_cume_dist",
        "status_code_dense_rank",
        "latency_ms_cume_dist_count_1h",
        "latency_ms_cume_dist_2",
        F.rank().over(w8).alias("latency_ms_cume_dist_2_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "status_code",
        "days_since_log_ts",
        "status_code_dense_rank",
        "latency_ms_cume_dist_count_1h",
        "latency_ms_cume_dist_2_rank",
        F.percent_rank().over(w9).alias("status_code_dense_rank_percent_rank"),
        F.sum("latency_ms_cume_dist_2_rank").over(w10).alias("latency_ms_cume_dist_2_rank_sum_30m"),
    )
)

result = (
    metrics
    .select("status_code", "status_code_dense_rank")
    .orderBy(F.col("status_code_dense_rank").desc())
)
