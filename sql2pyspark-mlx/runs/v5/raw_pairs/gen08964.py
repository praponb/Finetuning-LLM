from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        "store_id",
        F.sum("shipping_fee").over(w).alias("running_shipping_fee"),
        F.percent_rank().over(w2).alias("order_date_pct_rank"),
    )
    .orderBy(F.col("id").desc())
)
