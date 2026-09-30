from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "level", "latency_ms", "status_code", "k")
    .orderBy(F.col("service").desc_nulls_first())
)
