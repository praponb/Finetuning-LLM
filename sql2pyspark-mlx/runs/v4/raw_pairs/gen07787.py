from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .select("created_at", "product_id", "promo")
    .orderBy("created_at")
)
