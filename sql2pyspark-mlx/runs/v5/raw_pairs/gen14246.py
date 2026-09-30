from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .filter(F.col("discount").isNull())
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        F.round(F.avg("shipping_fee").over(w), 2).alias("shipping_fee_moving_avg_4"),
    )
)
