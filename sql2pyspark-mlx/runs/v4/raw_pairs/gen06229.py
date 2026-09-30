from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
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
        F.when(F.col("service").rlike("^(billing|auth)$"), "PRIORITY").when(F.col("service").rlike("^(search|checkout)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
