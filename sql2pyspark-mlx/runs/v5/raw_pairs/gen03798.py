from pyspark.sql import functions as F

flattened = (
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
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("service").rlike("^(checkout|auth)$"), "GROUP_A").when(F.col("service").rlike("^(billing|search)$"), "EXTENDED").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy("service", "service_zone")
)
