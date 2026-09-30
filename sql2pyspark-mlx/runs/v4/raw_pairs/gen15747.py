from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("service", "host")
w3 = Window.partitionBy("key")
w_recent = Window.partitionBy("service", "host").orderBy(F.col("host_count_latency_ms").asc_nulls_last())
w5 = Window.partitionBy("host", "key")
w6 = Window.partitionBy("service")
w_hist = Window.partitionBy("host").orderBy(F.col("key_max_latency_ms").desc_nulls_first())
w8 = Window.partitionBy("key", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.count("latency_ms").over(w).alias("host_count_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.max("latency_ms").over(w3).alias("key_max_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("host_count_latency_ms_dense_rank"),
        F.stddev_samp("latency_ms_share").over(w5).alias("host_stddev_samp_latency_ms_share"),
    )
)

metrics = (
    with_windows_app_logs
    .withColumn("service_min_key_max_latency_ms", F.min("key_max_latency_ms").over(w6))
    .withColumn("key_max_latency_ms_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("key_max_latency_ms_avg_1h", F.avg("key_max_latency_ms").over(w8))
)

filtered = (
    metrics
    .filter(F.col("log_ts") > F.lit("2025-06-02").cast("date"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        "v",
        "key_max_latency_ms",
        "latency_ms_share_2",
        "host_count_latency_ms_dense_rank",
        "service_min_key_max_latency_ms",
        "key_max_latency_ms_dense_rank",
        "key_max_latency_ms_avg_1h",
        F.upper("host").alias("host_upper"),
        F.concat("service", F.lit(", "), "host").alias("service_host"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("service") == "billing"))
    .withColumn("service_key", F.concat("service", F.lit("/"), "key"))
    .withColumn("service_group", F.when(F.col("service").isin(["auth", "checkout"]), "Group A").otherwise("Other"))
)

filtered_app_logs = (
    cleaned
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 12:00:00"))) & F.col("key").isNotNull() & (F.to_date("log_ts") < "2024-12-01"))
    .withColumn("key_lower", F.lower("key"))
    .withColumn("key_max_latency_ms_dense_rank_times_latency_ms_share_2", (F.col("key_max_latency_ms_dense_rank") * F.col("latency_ms_share_2")))
)

result = (
    filtered_app_logs
    .groupBy("service")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.round(F.sum("key_max_latency_ms_dense_rank") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("key_max_latency_ms_dense_rank_per_h1"),
        F.percentile_approx("key_max_latency_ms_dense_rank_times_latency_ms_share_2", [0.25, 0.5, 0.75]).alias("key_max_latency_ms_dense_rank_times_latency_ms_share_2_quartiles"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .filter(F.col("key_max_latency_ms_dense_rank_per_h1") >= 0)
)
