from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("level").isin(["WARN", "DEBUG"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    flattened_attrs
    .groupBy("host")
    .agg(
        F.countDistinct("trace").alias("distinct_traces"),
        F.percentile_approx("retry_count", 0.9).alias("retry_count_p90"),
        F.min("log_ts").alias("started_at"),
    )
    .orderBy(F.col("distinct_traces").desc_nulls_first())
)
