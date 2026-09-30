from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "service",
        "status_code",
        "k",
        "value",
        F.when(F.col("service").rlike("^(billing|search)$"), "EXTENDED").otherwise("CORE").alias("service_zone"),
    )
)
