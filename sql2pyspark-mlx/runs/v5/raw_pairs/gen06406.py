from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    lines
    .select("id")
    .orderBy(F.col("id").desc())
)
