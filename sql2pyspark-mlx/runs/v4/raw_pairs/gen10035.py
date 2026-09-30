from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .groupBy(
        "tier",
        F.when(F.col("latency_ms") >= 2111, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("is_flagged"))).alias("checkout_total_is_flagged"),
        F.avg("status_code_high").alias("avg_status_code_high"),
    )
    .orderBy(F.col("avg_status_code_high").asc_nulls_last())
)
