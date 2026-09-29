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
        "promo",
        F.when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "OTHER_ZONE").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_B").alias("promo_zone"),
    )
    .orderBy(F.col("qty").desc_nulls_first())
)
