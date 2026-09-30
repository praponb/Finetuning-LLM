from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("latency_ms") != 1721)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 1859, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.max("latency_ms").alias("max_latency_ms"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("latency_ms_per_search"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("latency_ms_per_billing"),
    )
    .filter(F.col("min_latency_ms") >= 0)
    .orderBy("min_latency_ms")
)
