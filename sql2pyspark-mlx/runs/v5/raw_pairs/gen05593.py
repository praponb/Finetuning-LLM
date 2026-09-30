from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "discount",
        (F.col("discount") - F.lag("discount", 1).over(w)).alias("discount_change"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("channel")
)
