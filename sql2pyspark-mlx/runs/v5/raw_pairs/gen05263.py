from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "host", "latency_ms", "key", "attr_value")
    .orderBy(F.col("host").desc_nulls_first(), "key")
)
