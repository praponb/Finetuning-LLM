from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
    )
)
