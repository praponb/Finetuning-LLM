from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.size("items").alias("num_items"),
        F.col("attributes")["device"].alias("device"),
    )
)
