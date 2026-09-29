from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)
