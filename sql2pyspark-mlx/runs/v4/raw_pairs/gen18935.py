from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)
