from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
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
        F.when(F.col("status_code") >= 387, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.min("log_ts").alias("started_at"),
    )
)
