from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.when(F.col("k").rlike("^(version|trace)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("k_zone"),
    )
)
