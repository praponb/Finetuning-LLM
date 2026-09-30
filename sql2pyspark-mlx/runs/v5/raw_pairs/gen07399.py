from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_attributes", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "coupon_codes",
        "used_welcome5",
        "max_item_price",
        "utm_source",
        "num_attributes",
        F.avg("max_item_price").over(w).alias("running_avg_max_item_price"),
        F.min("max_item_price").over(w).alias("running_min_max_item_price"),
        F.max("max_item_price").over(w).alias("running_max_max_item_price"),
    )
)
