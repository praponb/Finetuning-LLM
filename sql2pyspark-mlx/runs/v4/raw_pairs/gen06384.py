from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .groupBy(
        "service",
        "host",
        F.when(F.col("is_flagged") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_flagged_tier"),
    )
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.avg(F.when(F.col("service") == "checkout", F.col("retry_count"))).alias("checkout_avg_retry_count"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.sum(F.when(F.col("host") == "h4", F.col("retry_count")).otherwise(0)).alias("h4_retry_count"),
    )
    .filter(F.col("search_count") < 1)
    .orderBy(F.col("service").desc())
)
