from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .select(
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "PRIORITY").when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "GROUP_B").otherwise("EXTENDED").alias("promo_zone"),
    )
    .orderBy(F.col("product_id").desc(), "qty")
)
