from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(~F.col("host").isin(["h4", "h1", "h3"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "context",
        F.col("context")["version"].alias("version"),
        F.col("context").getItem("trace").alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .groupBy("host")
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.sum(F.when(F.col("level") == "WARN", F.col("duration_sec"))).alias("warn_total_duration_sec"),
    )
    .orderBy(F.col("latency_ms_bit_or").desc())
)
