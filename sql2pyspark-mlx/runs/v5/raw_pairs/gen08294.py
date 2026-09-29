from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.col("key").isNotNull() & F.col("level").isin(["INFO", "DEBUG", "ERROR"]))
    .select("*", F.concat("host", F.lit(" "), "level").alias("host_level"))
)

base = (
    filtered
    .filter(F.col("key").isNotNull() & F.col("key").isin(["trace", "version", "region"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        "value",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.substring("key", 1, 2).alias("key_prefix"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

filtered_app_logs = (
    base
    .filter(F.col("latency_ms") <= 2107)
    .select(
        "*",
        F.when(F.col("latency_ms") >= 2337, "High").when(F.col("latency_ms") >= 1161, "Medium").otherwise("Low").alias("latency_ms_band"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean_2"),
    )
)

kept_keys = (
    filtered_app_logs
    .select("service")
    .exceptAll(
        df_app_logs
        .filter(F.col("log_ts") < "2025-04-01")
        .select("service")
    )
)

kept_rows = (
    filtered_app_logs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "value",
        "level_prefix",
        "key_clean_2",
        F.when(F.col("key").rlike("^(version|trace)$"), "PRIORITY").when(F.col("key").rlike("^(region)$"), "EXTENDED").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy("key")
)
