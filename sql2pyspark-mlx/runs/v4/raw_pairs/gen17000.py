from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .groupBy("service")
    .agg(
        F.countDistinct("service").alias("distinct_services"),
        F.min("status_code").alias("min_status_code"),
    )
)
