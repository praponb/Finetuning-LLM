from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = lines.select("id", "level", "latency_ms", "attr_key", "value")
