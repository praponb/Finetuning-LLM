from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "discount",
        "channel",
        "amount",
        F.sum("discount").over(w).alias("running_discount"),
        F.row_number().over(w2).alias("amount_row"),
    )
)
