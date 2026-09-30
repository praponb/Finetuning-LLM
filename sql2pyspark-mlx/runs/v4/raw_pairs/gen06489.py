from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    flattened
    .select("id", "host", "log_ts", "latency_ms", "status_code", "k", "v")
    .orderBy(F.col("k").asc_nulls_last())
)
