from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode("context").alias("k", "value"))
)

result = (
    exploded
    .select("id", "host", "latency_ms", "k", "value")
    .orderBy(F.col("latency_ms").asc_nulls_last(), F.col("id").desc())
)
