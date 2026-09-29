from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "status",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_6"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
)
