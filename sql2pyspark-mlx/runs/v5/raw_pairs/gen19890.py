from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.size("items").alias("num_items"),
    )
)
