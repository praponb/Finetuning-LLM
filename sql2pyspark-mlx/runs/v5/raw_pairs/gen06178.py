from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "service",
        "latency_ms",
        "status_code",
        "attr_value",
        F.when(F.col("service").rlike("^(auth|search)$"), "CORE").when(F.col("service").rlike("^(billing|checkout)$"), "EXTENDED").otherwise("GROUP_B").alias("service_zone"),
    )
)
