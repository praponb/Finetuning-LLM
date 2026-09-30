from pyspark.sql import functions as F

result = (
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
