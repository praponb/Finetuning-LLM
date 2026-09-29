from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

matched_keys = (
    flattened
    .select("id", "level")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") != "INFO")
        .select("id", "level")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull() & (F.col("latency_ms") != 797))
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
)

result = (
    cleaned
    .select(
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "days_since_log_ts",
        F.when(F.col("level").rlike("^(INFO|DEBUG)$"), "GROUP_B").when(F.col("level").rlike("^(WARN|ERROR)$"), "CORE").otherwise("OTHER_ZONE").alias("level_zone"),
    )
)
