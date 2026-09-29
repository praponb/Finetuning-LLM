from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("service")
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.min("log_ts").alias("started_at"),
        F.count("*").alias("cnt"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
    )
    .filter(F.col("warn_count") < 1000)
    .orderBy(F.col("service").asc_nulls_last())
)
