from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

result = (
    flattened
    .select("id", "plan")
    .orderBy(F.col("id").desc())
)
