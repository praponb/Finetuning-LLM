from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

valid = (
    lines
    .filter(F.col("effective_from") <= "2025-02-01")
    .withColumn("clean_currency", F.trim("currency"))
)

base = (
    valid
    .filter(F.col("currency") == "USD")
    .withColumn("adjusted_tier_value", F.round((F.col("tier_value") * 1.15), 2))
    .withColumn("effective_from_weekofyear", F.weekofyear("effective_from"))
)

report = (
    base
    .select(
        "id",
        "effective_from",
        "tier_value",
        "clean_currency",
        "adjusted_tier_value",
    )
    .orderBy("effective_from", F.col("id").desc())
)

result = report.select("effective_from", "tier_value", "adjusted_tier_value")
