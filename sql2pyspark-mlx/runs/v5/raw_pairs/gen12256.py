from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    exploded
    .select("id")
    .orderBy(F.col("id").desc_nulls_last())
)
