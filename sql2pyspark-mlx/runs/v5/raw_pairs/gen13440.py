from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
