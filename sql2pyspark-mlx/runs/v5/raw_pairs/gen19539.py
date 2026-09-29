from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    lines
    .groupBy(
        "k",
        F.when(F.col("latency_ms") >= 1824, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        F.when(F.col("status_code") >= 414, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code")).otherwise(0)).alias("version_status_code"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.max("log_ts").alias("ended_at"),
    )
    .orderBy(F.col("span_minutes").desc())
)
