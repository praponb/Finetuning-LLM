from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    exploded
    .select("id", "created_at", "coupon_codes", "product_id", "qty", "price")
    .orderBy("product_id")
)
