from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .select("id", "plan", "currency", "tier_value")
    .orderBy(F.col("tier_value").desc())
)
