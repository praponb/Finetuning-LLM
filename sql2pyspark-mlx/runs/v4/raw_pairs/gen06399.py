from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 3.7, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("h1_count").asc_nulls_last())
)
