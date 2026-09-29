from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key").isin(["region", "trace"]), "Core").otherwise("Other").alias("attr_key_group"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

matched_keys = (
    valid
    .select("host", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("service").isin(["search", "billing", "checkout"]))
        .select("host", "level")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

valid_app_logs = (
    verified
    .filter(~F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "attr_key",
        "attr_key_group",
        "resolved_attr_key",
        F.when(F.col("level").isin(["DEBUG", "INFO"]), "Tier 1").otherwise("Other").alias("level_group"),
        F.lower("host").alias("host_lower"),
    )
)

result = (
    valid_app_logs
    .groupBy("attr_key")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
    )
    .orderBy("version_count")
)
