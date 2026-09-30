from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode("discounts").alias("val"))

result = (
    exploded
    .select("id", "val")
    .orderBy(F.col("val").desc_nulls_first(), "id")
)
