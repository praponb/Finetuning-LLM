from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("amount") <= 1170)
    .select(
        "id",
        "channel",
        "discount",
        "status",
        "order_date",
        F.sum("discount").over(w).alias("channel_sum_discount"),
        F.sum("discount").over(w2).alias("running_discount"),
    )
)
