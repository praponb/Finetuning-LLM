from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.zip").alias("ship_zip"),
        F.size("items").alias("num_items"),
    )
)
