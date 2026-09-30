from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h3"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
    )
)
