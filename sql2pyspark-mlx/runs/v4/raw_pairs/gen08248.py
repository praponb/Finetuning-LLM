from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id") == 5)
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
    )
)
