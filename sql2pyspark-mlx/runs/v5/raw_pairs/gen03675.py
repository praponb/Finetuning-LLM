from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

result = (
    exploded
    .select("id", "val")
    .orderBy(F.col("id").desc())
)
