from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("attributes")["gift"].alias("gift"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "created_at",
        "utm_source",
        "gift",
        "all_small_qty",
        F.last("utm_source", ignorenulls=True).over(w).alias("last_known_utm_source"),
    )
)
