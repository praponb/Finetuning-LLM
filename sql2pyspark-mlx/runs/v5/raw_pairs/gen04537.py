from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "channel",
        "status",
        "order_date",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
    )
)
