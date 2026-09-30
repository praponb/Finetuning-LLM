from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee_scaled", "id")
w3 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-11").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.round((F.col("shipping_fee") * 0.9), 2).alias("shipping_fee_scaled"))
)

result = (
    valid
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "shipping_fee_scaled",
        F.corr("discount", "shipping_fee").over(w).alias("corr_discount_shipping_fee"),
        F.ntile(5).over(w2).alias("shipping_fee_scaled_bucket"),
        F.last("discount", ignorenulls=True).over(w3).alias("last_known_discount"),
    )
)
