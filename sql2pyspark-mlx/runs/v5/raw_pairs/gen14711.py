from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = flattened.select("id", "service", "log_ts", "status_code", "k")
