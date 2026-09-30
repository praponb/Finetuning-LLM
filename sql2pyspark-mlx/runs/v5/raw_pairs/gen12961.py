from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .select("item_no", "product_id", "qty", "promo")
    .orderBy(F.col("item_no").desc(), F.col("product_id").desc())
)
