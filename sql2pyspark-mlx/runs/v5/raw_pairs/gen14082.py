from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("status_code") >= 468, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.min("log_ts").alias("started_at"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.min("latency_ms").alias("min_latency_ms"),
    )
)
