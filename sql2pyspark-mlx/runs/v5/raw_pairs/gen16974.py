from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

valid = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "level",
        "key",
        "value",
        F.when(F.col("key") == "version", 1).otherwise(0).alias("is_version"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("key").isin(["region", "version"]))
    .select(
        "id",
        "level",
        "key",
        "value",
        F.concat_ws("-", "level", "key").alias("level_key_key"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled_2"),
    )
)

base = (
    filtered
    .filter((F.col("key").eqNullSafe(F.lit("region"))) & F.col("key").isin(["version", "region", "trace"]))
    .select("*", F.trim("level").alias("clean_level"))
)

result = (
    base
    .groupingSets([["key", "level"], ["key"]], "key", "level")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .orderBy(F.col("key").desc_nulls_first())
)
