from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("element"))

result = (
    exploded
    .select("id", "plan", "element")
    .orderBy(F.col("element").desc_nulls_last())
)
