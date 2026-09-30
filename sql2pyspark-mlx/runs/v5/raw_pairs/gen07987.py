from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

result = (
    flattened
    .select("id", "effective_from", "val")
    .orderBy("val")
)
