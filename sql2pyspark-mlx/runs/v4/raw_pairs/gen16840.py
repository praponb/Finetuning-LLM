from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("latency_ms_count_1h").desc_nulls_last())
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

metrics = (
    flattened
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("latency_ms_sum_1h", F.sum("latency_ms").over(w2))
    .withColumn("latency_ms_count_1h", F.count("latency_ms").over(w2))
)

metrics_app_logs = (
    metrics
    .withColumn("latency_ms_avg_30m", F.avg("latency_ms").over(w_ordered))
    .withColumn("latency_ms_count_1h_dense_rank", F.dense_rank().over(w4))
    .withColumn("latency_ms_max_2h", F.max("latency_ms").over(w_running))
)

kept_keys = (
    metrics_app_logs
    .select("service", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("status_code") <= 317)
        .select("service", "id")
    )
)

reconciled = (
    metrics_app_logs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select("*", F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"))
)

enriched = (
    filtered
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4))
    .withColumn("attr_key_avg_days_since_log_ts", F.avg("days_since_log_ts").over(w7))
)

valid = (
    enriched
    .filter(F.col("log_ts") >= F.lit("2025-06-01 10:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "level",
        "v",
        "latency_ms_percent_rank",
        "latency_ms_sum_1h",
        "latency_ms_count_1h",
        "latency_ms_avg_30m",
        "latency_ms_count_1h_dense_rank",
        "latency_ms_max_2h",
        "latency_ms_share",
        "attr_key_avg_days_since_log_ts",
        F.round((F.col("latency_ms") / F.col("latency_ms_count_1h_dense_rank")), 2).alias("latency_ms_per_latency_ms_count_1h_dense_rank"),
    )
)

result = (
    valid
    .withColumn("latency_ms_max_2h_tier", F.when(F.col("latency_ms_max_2h") >= 2072, "HIGH").otherwise("LOW"))
    .withColumn("latency_ms_avg_30m_tier", F.when(F.col("latency_ms_avg_30m") >= 2004, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["service", "latency_ms_max_2h_tier", "latency_ms_avg_30m_tier"], ["service", "latency_ms_max_2h_tier"]],
        "service",
        "latency_ms_max_2h_tier",
        "latency_ms_avg_30m_tier",
    )
    .agg(
        F.avg(F.when(F.col("level") == "WARN", F.col("latency_ms_per_latency_ms_count_1h_dense_rank"))).alias("warn_avg_latency_ms_per_latency_ms_count_1h_dense_rank"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.avg("latency_ms_percent_rank"), 2).alias("avg_latency_ms_percent_rank"),
        F.sum("latency_ms_avg_30m").alias("total_latency_ms_avg_30m"),
    )
    .orderBy(F.col("latency_ms_avg_30m_tier").asc_nulls_first())
)
