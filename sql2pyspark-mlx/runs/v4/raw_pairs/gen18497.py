from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)
