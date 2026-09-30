from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .cube("tier")
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("tier") == "pro", 1)), F.lit(0)), 2).alias("latency_ms_per_pro"),
        F.count("*").alias("cnt"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.count(F.when(F.col("tier") == "gold", 1)).alias("gold_rows"),
        F.size(F.collect_set("tier")).alias("num_tiers"),
        F.sum(F.when(F.col("tier") == "free", F.col("latency_ms")).otherwise(0)).alias("free_latency_ms"),
    )
)
