from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") != 1834)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("latency_ms") >= 2099, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("tier") == "pro", 1)).alias("pro_rows"),
        F.percentile_approx("is_flagged", 0.95).alias("is_flagged_p95"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("is_flagged", 0.9).alias("is_flagged_p90"),
    )
    .orderBy(F.col("latency_ms_bit_and").desc_nulls_first())
)
