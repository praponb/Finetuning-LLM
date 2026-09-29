from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
    )
)
