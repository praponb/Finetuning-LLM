from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2138, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["level", "latency_ms_tier"], ["level"], []],
        "level",
        "latency_ms_tier",
    )
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("latency_ms_per_error"),
    )
    .filter(F.col("debug_count") >= 1)
)
