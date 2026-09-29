from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.col("context")["region"].alias("region"),
    )
)
