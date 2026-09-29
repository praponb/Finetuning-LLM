from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .groupBy("promo")
    .agg(
        F.min("created_at").alias("first_seen"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .orderBy(F.col("promo").desc_nulls_first())
)
