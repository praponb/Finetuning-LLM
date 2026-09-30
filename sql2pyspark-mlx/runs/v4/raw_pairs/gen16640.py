from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")
w3 = Window.orderBy(F.col("created_at").desc(), "id")
w_recent = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.col("attributes")["device"].alias("device"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.last("device_or_default", ignorenulls=True).over(w).alias("last_known_device_or_default"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("device", ignorenulls=True).over(w_recent).alias("first_known_device"),
        F.last("device", ignorenulls=True).over(w_recent).alias("last_known_device"),
    )
)
