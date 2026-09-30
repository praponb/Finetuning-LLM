from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select("service", "log_ts", "latency_ms", "value")
    .orderBy(F.col("service").desc())
)
