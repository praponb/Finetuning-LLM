from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select("id", "service", "level", "latency_ms", "status_code", "k")
    .orderBy(F.col("level").desc())
)
