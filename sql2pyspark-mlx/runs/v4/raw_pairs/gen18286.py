from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.size("attributes").alias("num_attributes"),
    )
)
