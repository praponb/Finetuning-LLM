from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "k",
        F.when(F.col("k").rlike("^(trace|region)$"), "PRIORITY").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("k").asc_nulls_first(), "k_zone")
)
