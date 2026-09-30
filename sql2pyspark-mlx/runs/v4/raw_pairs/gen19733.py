from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.size("items").alias("num_items"),
        F.col("attributes")["gift"].alias("gift"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
    )
)
