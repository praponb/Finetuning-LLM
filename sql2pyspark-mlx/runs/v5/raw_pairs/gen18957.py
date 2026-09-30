from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service").isin(["checkout", "auth", "search"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 2669, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.count_if(F.col("tier") == "pro").alias("pro_events"),
        F.count(F.when(F.col("tier") == "pro", 1)).alias("pro_count"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("latency_ms_per_h1"),
        F.countDistinct("tier").alias("distinct_tiers"),
    )
    .filter(F.col("pro_count") >= 1)
)
