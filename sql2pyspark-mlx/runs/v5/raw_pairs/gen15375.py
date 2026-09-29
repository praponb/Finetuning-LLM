from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([13, 37]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
