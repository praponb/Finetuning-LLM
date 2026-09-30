from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy("host")
    .agg(
        F.sum("retry_count").alias("total_retry_count"),
        F.max(F.when(F.col("host") == "h1", F.col("retry_count"))).alias("h1_max_retry_count"),
    )
    .orderBy(F.col("h1_max_retry_count").desc())
)
