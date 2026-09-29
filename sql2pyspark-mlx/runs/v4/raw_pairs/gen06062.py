from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .select("id", "tier_value")
    .orderBy(F.col("tier_value").desc())
)
