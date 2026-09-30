from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") == 1715)
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "context",
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("trace").alias("trace"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    unpacked
    .withColumn("has_fault_tier", F.when(F.col("has_fault") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("service", "level", "has_fault_tier")
    .agg(
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.grouping("service").alias("grouping_level"),
    )
    .orderBy(F.col("has_fault_tier").desc())
)
