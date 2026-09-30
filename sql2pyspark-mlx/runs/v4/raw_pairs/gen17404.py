from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode("context").alias("attr_key", "v"))
)

result = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc_nulls_last(), "service")
)
