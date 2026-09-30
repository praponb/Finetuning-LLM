from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
