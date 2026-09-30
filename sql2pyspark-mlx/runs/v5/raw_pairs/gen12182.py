from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("latency_ms") >= 2194, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
        "key",
    )
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("latency_ms_per_error"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .filter(F.col("latency_ms_per_error") > 0)
)
