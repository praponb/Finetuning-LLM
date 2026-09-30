from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "value",
        F.when(F.col("service").rlike("^(checkout|auth)$"), "GROUP_B").when(F.col("service").rlike("^(billing|search)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("service_zone"),
    )
    .orderBy(F.col("id").desc())
)
