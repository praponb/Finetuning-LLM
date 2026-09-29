from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

result = (
    exploded
    .select("id")
    .orderBy(F.col("id").desc_nulls_last())
)
