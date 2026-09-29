from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

result = (
    flattened
    .select("id", "product_id", "qty", "price", "promo")
    .orderBy(F.col("product_id").desc(), "id")
)
