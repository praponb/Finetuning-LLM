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
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "v",
        F.when(F.col("key").rlike("^(version|region)$"), "EXTENDED").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("level").desc(), F.col("log_ts").desc())
)
