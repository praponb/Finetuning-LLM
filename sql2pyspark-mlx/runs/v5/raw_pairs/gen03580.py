from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        F.when(F.col("service").rlike("^(checkout|billing)$"), "GROUP_B").when(F.col("service").rlike("^(auth|search)$"), "OTHER_ZONE").otherwise("CORE").alias("service_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
