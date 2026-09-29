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
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .select("host", "level", "status_code", "key", "v")
    .orderBy(F.col("host").desc_nulls_first(), "status_code")
)
