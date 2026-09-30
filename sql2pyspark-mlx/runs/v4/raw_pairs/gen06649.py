from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("attributes")["gift"].alias("gift"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.size("items").alias("num_items"),
    )
)
