from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        F.date_trunc("day", "log_ts").alias("activity_day"),
    )
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.avg(F.when(F.col("host") == "h1", F.col("latency_ms"))).alias("h1_avg_latency_ms"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
    )
)
