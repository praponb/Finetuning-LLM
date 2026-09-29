from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select("id", "log_ts", "level", "latency_ms", "k")
    .orderBy(F.col("id").asc_nulls_first())
)
