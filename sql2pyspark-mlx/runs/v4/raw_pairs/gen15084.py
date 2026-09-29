from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

result = (
    exploded
    .select("effective_from")
    .orderBy(F.col("effective_from").asc_nulls_first())
)
