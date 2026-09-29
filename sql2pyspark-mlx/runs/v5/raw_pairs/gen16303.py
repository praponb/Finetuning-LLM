from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("latency_ms") >= 1809, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.max(F.when(F.col("service") == "billing", F.col("latency_ms"))).alias("billing_max_latency_ms"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
    )
    .filter(F.col("billing_count") < 1000)
    .orderBy("latency_ms_tier")
)
