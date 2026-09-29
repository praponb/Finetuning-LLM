from pyspark.sql import functions as F

exploded = (
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
    exploded
    .select("id", "service", "host", "level", "latency_ms", "k", "attr_value")
    .orderBy(F.col("k").asc_nulls_first())
)
