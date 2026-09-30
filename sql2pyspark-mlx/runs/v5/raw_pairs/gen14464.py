from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.size("items").alias("num_items"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
