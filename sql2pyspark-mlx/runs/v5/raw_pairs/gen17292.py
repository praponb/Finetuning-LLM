from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("created_at", "id")

parsed = (
    df_carts
    .filter(F.col("created_at") >= "2024-08-01")
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    parsed
    .filter(~F.col("has_premium_item"))
    .select(
        "id",
        "created_at",
        "device_or_default",
        "has_premium_item",
        "used_spring10",
        F.first("device_or_default", ignorenulls=True).over(w).alias("first_known_device_or_default"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)
