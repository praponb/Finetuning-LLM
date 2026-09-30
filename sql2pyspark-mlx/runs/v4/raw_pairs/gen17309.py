from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("channel_avg_shipping_fee"),
    )
)
