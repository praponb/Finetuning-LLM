from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("promo_zone"),
    )
    .orderBy(F.col("id").desc())
)
