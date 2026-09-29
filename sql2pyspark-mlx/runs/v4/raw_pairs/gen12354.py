from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "service", "log_ts", "latency_ms", "k", "attr_value")
    .orderBy("k")
)
