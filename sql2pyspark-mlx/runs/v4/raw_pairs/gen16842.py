from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = exploded.select("id", "customer_id", "created_at", "product_id", "qty")
