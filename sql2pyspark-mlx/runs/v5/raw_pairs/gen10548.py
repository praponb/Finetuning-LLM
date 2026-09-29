from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    flattened
    .select(
        "service",
        "level",
        "latency_ms",
        F.when(F.col("service").rlike("^(search|checkout)$"), "PRIORITY").when(F.col("service").rlike("^(auth|billing)$"), "EXTENDED").otherwise("CORE").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").asc_nulls_first(), "latency_ms")
)
