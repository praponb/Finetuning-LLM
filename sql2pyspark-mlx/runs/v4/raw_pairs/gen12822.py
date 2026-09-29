from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2520, "LARGE").otherwise("SMALL"))
    .rollup("latency_ms_tier")
    .agg(
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.min("log_ts").alias("first_seen"),
        F.count(F.when(F.col("tier") == "pro", 1)).alias("pro_rows"),
        F.grouping("latency_ms_tier").alias("gid"),
    )
    .orderBy(F.col("pro_rows").desc())
)
