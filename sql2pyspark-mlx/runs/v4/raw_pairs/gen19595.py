from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

base = (
    exploded
    .filter(F.col("attr_key") == "version")
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.when(F.col("level") == "WARN", 1).otherwise(0).alias("is_warn"),
        F.when(F.col("service").isin(["search", "checkout"]), "Core").otherwise("Other").alias("service_group"),
    )
)

filtered = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

matched_keys = (
    filtered
    .select("level", "id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") != 1348)
        .select("level", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("level", "latency_ms", "is_warn", "service_group", "attr_key_filled")
)
