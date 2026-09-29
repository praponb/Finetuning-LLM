from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "host", "log_ts", "latency_ms", "status_code", "key", "attr_value")
    .orderBy(F.col("status_code").desc())
)
