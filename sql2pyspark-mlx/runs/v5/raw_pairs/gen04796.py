from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)

result = exploded.select("id", "effective_from", "element")
