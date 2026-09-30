from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

result = (
    flattened
    .select("id", "val")
    .orderBy(F.col("val").desc())
)
