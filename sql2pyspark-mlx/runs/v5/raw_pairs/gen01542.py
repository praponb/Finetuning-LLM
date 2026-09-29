from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel", "store_id")
w_hist = Window.partitionBy("channel").orderBy("discount", "id")
w3 = Window.orderBy(F.col("amount").desc_nulls_first())
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.count("shipping_fee").over(w_recent).alias("channel_count_shipping_fee"),
        F.ntile(3).over(w_hist).alias("discount_bucket"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.first("shipping_fee", ignorenulls=True).over(w4).alias("first_known_shipping_fee"),
    )
)
