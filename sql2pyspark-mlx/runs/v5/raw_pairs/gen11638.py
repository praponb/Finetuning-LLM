from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 3.5, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("day", "log_ts").alias("activity_day"),
    )
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.countDistinct("trace_or_default").alias("distinct_trace_or_defaults"),
        F.count("*").alias("row_count"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.min("log_ts").alias("started_at"),
    )
    .filter(F.col("latency_ms_bit_and") > 1)
    .orderBy(F.col("latency_ms_bit_and").asc_nulls_first())
)
