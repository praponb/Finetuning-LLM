from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .groupBy("service")
    .agg(
        F.percentile_approx("retry_count", [0.25, 0.5, 0.75]).alias("retry_count_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
    )
    .orderBy(F.col("error_count").desc())
)
