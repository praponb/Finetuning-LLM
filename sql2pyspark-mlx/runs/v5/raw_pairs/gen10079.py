from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    parsed
    .select("id", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") != 374)
        .select("id", "level")
    )
)

verified = (
    parsed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("attr_key").isNotNull())
    .withColumn("log_ts_quarter", F.quarter("log_ts"))
    .withColumn("log_ts_period", F.date_trunc("week", "log_ts"))
)

remaining_keys = (
    filtered
    .select("id", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("level") == "INFO")
        .select("id", "level")
    )
)

verified_app_logs = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified_app_logs
    .select(
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "has_fault",
        "status_code_high",
        "log_ts_period",
    )
)
