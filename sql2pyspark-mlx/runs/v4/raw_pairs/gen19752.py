from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 1945, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        "service",
    )
    .agg(
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.sum(F.when(F.col("service") == "auth", F.col("latency_ms"))).alias("auth_total_latency_ms"),
    )
    .orderBy(F.col("latency_ms_tier").desc_nulls_first())
)
