from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("day", "log_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("activity_day").desc_nulls_first())
)
