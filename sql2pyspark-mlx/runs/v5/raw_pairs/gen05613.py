from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
    )
)
