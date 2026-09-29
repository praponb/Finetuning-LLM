from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("entry.qty"),
        F.col("entry.promo"),
    )
)
