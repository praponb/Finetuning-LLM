from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)
