from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select("host", "latency_ms", "status_code", "attr_key", "attr_value")
    .orderBy(F.col("status_code").desc(), F.col("attr_key").desc())
)
