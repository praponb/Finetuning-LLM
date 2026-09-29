from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select("id", F.col("li_item.product_id"), F.col("li_item.promo"))
)

result = (
    exploded
    .select("id")
    .orderBy(F.col("id").desc_nulls_first())
)
