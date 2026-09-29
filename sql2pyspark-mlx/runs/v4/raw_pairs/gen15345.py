from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    lines
    .filter(F.col("attr_key") == "version")
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

matched_keys = (
    unpacked
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["WARN", "DEBUG"]))
        .select("id")
    )
)

reconciled = (
    unpacked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        "v",
        "status_code_high",
        "has_fault",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("attr_key_zone"),
    )
)
