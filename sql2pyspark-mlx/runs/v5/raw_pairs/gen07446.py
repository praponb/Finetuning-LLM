from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .select("id", "level", "latency_ms", "status_code", "key", "v")
    .orderBy(F.col("id").desc_nulls_first())
)
