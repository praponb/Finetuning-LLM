from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .select("id", "host", "level", "k", "v")
    .orderBy(F.col("id").asc_nulls_last(), "level")
)
