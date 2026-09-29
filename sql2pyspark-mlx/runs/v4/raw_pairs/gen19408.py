from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2484, "LARGE").otherwise("SMALL"))
    .rollup("latency_ms_tier", "service")
    .agg(
        F.percentile_approx("retry_count", 0.5).alias("retry_count_p50"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.grouping("latency_ms_tier").alias("grouping_id"),
    )
    .filter(F.col("retry_count_p50") >= 0)
    .orderBy(F.col("latency_ms_tier").desc())
)
