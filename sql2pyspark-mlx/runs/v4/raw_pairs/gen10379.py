from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "context",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("duration_sec") >= 4.1, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max(F.when(F.col("service") == "billing", F.col("duration_sec"))).alias("billing_max_duration_sec"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
    .orderBy("num_services")
)
