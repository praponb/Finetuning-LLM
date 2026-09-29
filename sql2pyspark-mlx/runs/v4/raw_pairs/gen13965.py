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
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select("id", "host", "log_ts", "level", "latency_ms", "attr_key", "attr_value")
    .orderBy(F.col("log_ts").desc_nulls_first())
)
