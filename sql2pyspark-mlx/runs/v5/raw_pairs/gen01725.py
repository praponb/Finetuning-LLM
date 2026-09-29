from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)
