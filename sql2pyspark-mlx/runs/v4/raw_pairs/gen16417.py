from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)
