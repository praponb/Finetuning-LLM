from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.promo"),
    )
)
