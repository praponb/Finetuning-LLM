from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy("level", "host")
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count("*").alias("event_count"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.count("*").alias("row_count"),
    )
)
