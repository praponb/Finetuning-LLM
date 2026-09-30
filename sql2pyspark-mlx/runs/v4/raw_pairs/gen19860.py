from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"), "service")
    .agg(
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
    .orderBy(F.col("latency_ms_p50").desc())
)
