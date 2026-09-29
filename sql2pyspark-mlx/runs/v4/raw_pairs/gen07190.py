from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .select("qty", "price", "promo")
    .orderBy(F.col("promo").desc_nulls_first())
)
