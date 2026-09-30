from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .select(
        "id",
        "coupon_codes",
        "line_no",
        "qty",
        "promo",
        F.when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "GROUP_A").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "CORE").otherwise("GROUP_B").alias("promo_zone"),
    )
    .orderBy(F.col("promo_zone").desc())
)
