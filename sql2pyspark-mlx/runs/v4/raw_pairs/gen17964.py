from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") > 645)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)
