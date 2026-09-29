from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|SPRING10)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("promo_zone"),
    )
    .orderBy("promo_zone")
)
