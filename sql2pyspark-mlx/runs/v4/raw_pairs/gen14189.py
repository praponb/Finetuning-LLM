from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "level",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.col("context").getItem("version").alias("version"),
    )
)
