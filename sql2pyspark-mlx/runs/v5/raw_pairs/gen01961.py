from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("basket_amount", "id")
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-5, -1)

unpacked = (
    df_carts
    .filter(F.col("created_at") < "2024-06-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.col("attributes")["gift"].alias("gift"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "utm_source",
        "basket_amount",
        F.first("weight_kg", ignorenulls=True).over(w).alias("first_known_weight_kg"),
        F.last("weight_kg", ignorenulls=True).over(w2).alias("last_known_weight_kg"),
        F.ntile(3).over(w_recent).alias("basket_amount_bucket"),
        F.avg("basket_amount").over(w4).alias("basket_amount_avg_5_prior"),
    )
)
