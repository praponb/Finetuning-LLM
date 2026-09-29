from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "web", "store"]))
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        "store_id",
        F.lead("shipping_fee", 1, 0).over(w).alias("next_shipping_fee"),
        F.sum("shipping_fee").over(w2).alias("store_id_sum_shipping_fee"),
    )
)
