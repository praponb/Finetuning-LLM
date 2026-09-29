from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "service", F.explode_outer("context").alias("key", "value"))
)

valid = (
    exploded
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "service", "key").alias("service_key_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

cleaned = (
    valid
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "value",
        "service_key_key",
        "resolved_key",
        F.when(F.col("key") == "region", 1).otherwise(0).alias("is_region"),
    )
)

kept_keys = (
    cleaned
    .select("service", "id")
    .subtract(
        df_app_logs
        .filter(F.to_date("log_ts") < "2023-07-01")
        .select("service", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy(
        F.when(F.col("is_region") >= 74, "HIGH").otherwise("LOW").alias("is_region_tier"),
        "service",
    )
    .agg(
        F.percentile_approx("is_region", [0.25, 0.5, 0.75]).alias("is_region_quartiles"),
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
        F.max(F.when(F.col("service") == "auth", F.col("is_region"))).alias("auth_max_is_region"),
    )
    .filter(F.col("auth_max_is_region") >= 0)
)

result = (
    daily
    .select("is_region_tier", "service", "is_region_quartiles", "auth_max_is_region")
    .orderBy(F.col("is_region_tier").desc())
)
