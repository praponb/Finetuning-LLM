from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select("id", "host", "log_ts", "latency_ms", "status_code", "attr_key", "value")
    .orderBy("log_ts", "host")
)
