from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select("id", "service", "latency_ms", "k")
    .orderBy(F.col("latency_ms").asc_nulls_first())
)
