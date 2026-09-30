from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
    )
)
