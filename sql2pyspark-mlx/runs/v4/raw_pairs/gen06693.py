from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status", "channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "channel",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("amount", "order_date")
)
