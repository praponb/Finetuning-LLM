from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = exploded.select("id", "effective_from", "tier_value")
