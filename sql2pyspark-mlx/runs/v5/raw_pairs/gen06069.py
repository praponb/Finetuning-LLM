from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("service").rlike("^(search|billing)$"), "GROUP_A").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy("k")
)
