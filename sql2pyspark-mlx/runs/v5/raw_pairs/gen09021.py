from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
    )
)
