from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

result = (
    lines
    .select("id", "currency", "val")
    .orderBy(F.col("val").desc())
)
