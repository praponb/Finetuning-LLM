from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(~F.col("service").isin(["auth", "checkout"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
    )
)
