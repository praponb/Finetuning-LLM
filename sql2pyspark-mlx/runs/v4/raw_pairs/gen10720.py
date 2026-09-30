from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("tier_value"))

result = (
    flattened
    .select("tier_value")
    .orderBy(F.col("tier_value").asc_nulls_last())
)
