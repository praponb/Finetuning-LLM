from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["gift"].alias("gift"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)
