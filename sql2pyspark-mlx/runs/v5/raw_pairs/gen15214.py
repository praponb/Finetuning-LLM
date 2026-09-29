from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .select("id", "latency_ms", "key", "attr_value")
    .orderBy("key")
)
