from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("ship_to.city").alias("ship_city"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
    )
)
