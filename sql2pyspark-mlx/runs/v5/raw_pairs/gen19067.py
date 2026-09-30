from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "key",
        "v",
        F.when(F.col("key").rlike("^(trace|version)$"), "PRIORITY").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("log_ts").asc_nulls_first())
)
