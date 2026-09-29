from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id") == 8)
    .select(
        "id",
        "customer_id",
        "attributes",
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.size("items").alias("num_items"),
    )
)
