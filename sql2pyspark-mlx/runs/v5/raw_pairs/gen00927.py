from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
    )
)
