from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = exploded.select("id", "coupon_codes", "pos", "product_id", "qty", "price", "promo")
