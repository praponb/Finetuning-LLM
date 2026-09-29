from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("status_code") > 424)
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("latency_ms") >= 1974, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.sum(F.when(F.col("service") == "checkout", F.col("status_code_high"))).alias("checkout_total_status_code_high"),
    )
    .filter(F.col("billing_count") > 1)
    .orderBy("billing_count")
)
