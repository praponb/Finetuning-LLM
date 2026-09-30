from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("status_code_high", 0.95).alias("status_code_high_p95"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.count(F.when(F.col("tier") == "gold", 1)).alias("gold_rows"),
    )
    .filter(F.col("distinct_tiers") > 1)
    .orderBy(F.col("gold_rows").desc())
)
