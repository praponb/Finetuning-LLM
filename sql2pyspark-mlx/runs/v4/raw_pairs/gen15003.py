from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code").between(282, 376))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    parsed
    .cube("level")
    .agg(
        F.percentile_approx("is_flagged", 0.5).alias("is_flagged_p50"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.percentile_approx("is_flagged", 0.9).alias("is_flagged_p90"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("corr_status_code_latency_ms").asc_nulls_first())
)
