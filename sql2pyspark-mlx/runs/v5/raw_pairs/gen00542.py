from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)
