from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id")
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select(
        "id",
        "channel",
        "store_id",
        "shipping_fee",
        "order_date",
        F.min("shipping_fee").over(w).alias("channel_min_shipping_fee"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
