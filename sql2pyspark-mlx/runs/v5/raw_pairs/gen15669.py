from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .select("id", "val")
    .orderBy(F.col("id").desc())
)
