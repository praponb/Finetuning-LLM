from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "pos",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "GROUP_A").when(F.col("promo").rlike("^(SPRING10|BUNDLE)$"), "CORE").otherwise("GROUP_B").alias("promo_zone"),
    )
)
