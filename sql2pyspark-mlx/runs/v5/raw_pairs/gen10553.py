from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

kept_keys = (
    lines
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms") != 608)
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp"))
    .withColumn("is_trace", F.when(F.col("attr_key") == "trace", 1).otherwise(0))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

filtered_app_logs = (
    filtered
    .filter(F.hour("log_ts").between(6, 9))
    .select(
        "*",
        F.weekofyear("log_ts").alias("log_ts_weekofyear"),
        F.length("attr_key").alias("attr_key_length"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "log_ts",
        "attr_key",
        "attr_value",
        "is_trace",
        "attr_key_filled",
        "log_ts_weekofyear",
        "attr_key_length",
        "resolved_attr_key",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "GROUP_A").when(F.col("attr_key").rlike("^(trace)$"), "CORE").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("is_trace").desc())
)
