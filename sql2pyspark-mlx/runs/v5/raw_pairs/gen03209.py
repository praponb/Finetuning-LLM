from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select("id", "host", "attr_key", "value")
    .orderBy(F.col("id").asc_nulls_first(), F.col("attr_key").desc())
    .limit(5)
)
