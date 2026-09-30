from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(-5, -1)

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    unpacked
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupons_text",
        "has_welcome_code",
        "ship_zip",
        F.sum("max_item_price").over(w).alias("cumulative_max_item_price"),
        F.sum("max_item_price").over(w_part).alias("cumulative_max_item_price_2"),
        F.avg("max_item_price").over(w3).alias("max_item_price_avg_5"),
        F.avg("max_item_price").over(w4).alias("max_item_price_avg_5_prior"),
    )
)
