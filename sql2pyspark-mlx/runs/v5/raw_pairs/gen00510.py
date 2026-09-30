from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("currency") == "THB")
    .select("*", F.round((F.col("tier_value") * 0.9), 2).alias("tier_value_scaled"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("plan") == "pro")
    .withColumn("tier_value_scaled_plus_tier_value", (F.col("tier_value_scaled") + F.col("tier_value")))
)

cleaned = (
    filtered_price_tiers
    .filter(F.col("tier_value").between(60, 70))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "tier_value_scaled",
        F.round((F.col("tier_value_scaled_plus_tier_value") * 1.1), 2).alias("tier_value_scaled_plus_tier_value_scaled"),
        (F.col("tier_value") * F.col("tier_value_scaled")).alias("tier_value_times_tier_value_scaled"),
    )
)

valid = (
    cleaned
    .filter(F.col("tier_value_scaled_plus_tier_value_scaled").between(21, 65))
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_scaled",
        "tier_value_scaled_plus_tier_value_scaled",
        "tier_value_times_tier_value_scaled",
        F.round("tier_value_scaled_plus_tier_value_scaled", 0).alias("tier_value_scaled_plus_tier_value_scaled_rounded"),
        (F.col("tier_value_scaled") + F.col("tier_value")).alias("tier_value_scaled_plus_tier_value"),
    )
)

result = (
    valid
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_scaled",
        "tier_value_scaled_plus_tier_value_scaled",
        "tier_value_times_tier_value_scaled",
        "tier_value_scaled_plus_tier_value_scaled_rounded",
        "tier_value_scaled_plus_tier_value",
    )
    .orderBy(
        "tier_value_scaled_plus_tier_value_scaled_rounded",
        "tier_value_scaled_plus_tier_value",
    )
)
