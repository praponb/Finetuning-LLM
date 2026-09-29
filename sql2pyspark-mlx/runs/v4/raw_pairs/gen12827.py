from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
