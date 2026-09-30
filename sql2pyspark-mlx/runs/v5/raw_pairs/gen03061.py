from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
    )
)
