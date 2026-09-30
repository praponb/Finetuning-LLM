from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select("*", F.trim("key").alias("clean_key"))
)

filtered_app_logs = (
    filtered
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select("*", F.coalesce("key", F.lit("unassigned")).alias("key_filled"))
)

result = (
    filtered_app_logs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"), "key")
    .agg(
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.sum(F.when(F.col("key") == "trace", F.col("has_fault")).otherwise(0)).alias("trace_has_fault"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.min("log_ts").alias("first_seen"),
    )
)
