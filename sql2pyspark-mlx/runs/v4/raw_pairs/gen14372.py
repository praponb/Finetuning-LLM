from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
