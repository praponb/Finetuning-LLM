from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .filter(F.col("key") != "version")
    .select(
        "log_ts",
        "level",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

base = (
    parsed
    .filter(F.col("status_code_high") != 2)
    .select(
        "log_ts",
        "level",
        "key",
        "status_code_high",
        F.length("level").alias("level_length"),
    )
)

result = (
    base
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        "key",
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.max("log_ts").alias("last_log_ts"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
    )
    .orderBy(F.col("region_rows").desc())
)
