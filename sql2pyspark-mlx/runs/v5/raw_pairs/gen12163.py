from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)
