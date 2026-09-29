from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "coupon_codes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["device"].alias("device"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)
