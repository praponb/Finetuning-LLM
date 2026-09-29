from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode("tier_limits").alias("val"))

result = (
    exploded
    .select("id", "val")
    .orderBy(F.col("val").desc_nulls_first())
)
