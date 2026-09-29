from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "PRIORITY").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_B").alias("promo_zone"),
    )
    .orderBy("id")
)
