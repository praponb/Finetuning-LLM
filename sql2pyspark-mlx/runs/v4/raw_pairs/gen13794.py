from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        F.when(F.col("service").rlike("^(checkout|auth)$"), "CORE").when(F.col("service").rlike("^(billing|search)$"), "GROUP_A").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").asc_nulls_first(), "latency_ms")
)
