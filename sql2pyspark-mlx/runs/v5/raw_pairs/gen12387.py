from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
