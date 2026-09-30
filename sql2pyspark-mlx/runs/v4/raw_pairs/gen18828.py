from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "status_code",
        "payload_json",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("status_code") >= 460, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.round(F.avg("num_context"), 2).alias("avg_num_context"),
    )
    .filter(F.col("avg_num_context") >= 0)
    .orderBy("status_code_tier")
)
