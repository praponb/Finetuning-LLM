from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
    )
)
