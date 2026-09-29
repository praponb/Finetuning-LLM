from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .select("plan")
    .orderBy(F.col("plan").desc_nulls_first())
)
