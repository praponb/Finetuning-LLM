from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = flattened.select("id", "element")
