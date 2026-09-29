from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.size("attributes").alias("num_attributes"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
