from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("service") == "auth", F.col("latency_ms")).otherwise(0)).alias("auth_latency_ms"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
