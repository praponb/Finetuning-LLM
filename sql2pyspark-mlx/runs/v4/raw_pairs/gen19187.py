from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    lines
    .select(
        "customer_id",
        "created_at",
        "coupon_codes",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(BUNDLE|VIP)$"), "GROUP_A").when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("CORE").alias("promo_zone"),
    )
    .orderBy(F.col("promo").desc_nulls_last(), "created_at")
)
