from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)
