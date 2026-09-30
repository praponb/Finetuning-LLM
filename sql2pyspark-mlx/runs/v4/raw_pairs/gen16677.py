from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        F.when(F.col("k").rlike("^(region|trace)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("k").desc())
)
