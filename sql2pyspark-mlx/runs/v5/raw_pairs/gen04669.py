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
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2368, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.countDistinct("k").alias("distinct_ks"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .orderBy(F.col("hour_window").desc())
)
