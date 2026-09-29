from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
    .filter(F.col("auth_rows") > 1)
    .orderBy("latency_ms_p95")
)
