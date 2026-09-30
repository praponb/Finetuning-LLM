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
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select("id", "service", "host", "log_ts", "latency_ms", "attr_key", "v")
    .orderBy(F.col("log_ts").desc_nulls_last())
)
