from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select("service", "level", "latency_ms", "key", "v")
    .orderBy(F.col("latency_ms").desc())
)
