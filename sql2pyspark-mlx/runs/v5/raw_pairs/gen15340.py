from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .groupBy("key")
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.countDistinct("host").alias("distinct_hosts"),
    )
    .filter(F.col("distinct_hosts") >= 1)
    .orderBy(F.col("distinct_hosts").asc_nulls_first())
)
