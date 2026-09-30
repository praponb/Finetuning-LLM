from pyspark.sql import functions as F

lines = (
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

flattened_attrs = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

matched_keys = (
    flattened_attrs
    .select("host", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms").isNotNull())
        .select("host", "id")
    )
)

kept_rows = (
    flattened_attrs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("status_code_high") <= 2)
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "key",
        "status_code_high",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

result = filtered.select("id", "host", "key", "status_code_high", "log_ts_day")
