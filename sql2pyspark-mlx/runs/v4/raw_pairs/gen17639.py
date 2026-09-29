from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)
