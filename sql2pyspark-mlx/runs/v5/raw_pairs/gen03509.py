from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

result = (
    lines
    .select("id", "currency", "val")
    .orderBy(F.col("currency").desc(), "val")
)
