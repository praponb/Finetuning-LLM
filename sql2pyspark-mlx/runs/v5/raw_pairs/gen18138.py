from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded = lines.select("qty", "promo", F.explode("coupon_codes").alias("element"))

base = (
    exploded
    .filter(F.col("promo") != "BUNDLE")
    .withColumn("element_promo", F.concat("element", F.lit(" "), "promo"))
)

valid = (
    base
    .filter(F.col("element") == "SPRING10")
    .select(
        "*",
        F.concat_ws("/", "element", "promo").alias("element_promo_key"),
        F.coalesce("element", F.lit("N/A")).alias("element_filled"),
    )
)

valid_carts = (
    valid
    .filter(F.col("element").isNotNull() & (~F.col("element").eqNullSafe(F.lit("BUNDLE"))))
    .withColumn("is_bundle", F.when(F.col("element") == "BUNDLE", 1).otherwise(0))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("DIRECT")))
)

result = (
    valid_carts
    .select(
        "qty",
        "promo",
        "element_promo_key",
        "element_filled",
        "is_bundle",
        F.when(F.col("promo").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(F.col("qty").desc())
)
