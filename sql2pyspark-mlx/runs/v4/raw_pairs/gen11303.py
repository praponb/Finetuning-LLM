from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .groupingSets([["tier", "host"], ["tier"], []], "tier", "host")
    .agg(
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("status_code_per_auth"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("tier").asc_nulls_last())
)
