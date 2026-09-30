from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2023-04-13")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
    )
)
