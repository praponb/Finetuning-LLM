from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.col("context")["region"].alias("region"),
    )
)
