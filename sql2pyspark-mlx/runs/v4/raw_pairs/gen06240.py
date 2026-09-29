from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)
