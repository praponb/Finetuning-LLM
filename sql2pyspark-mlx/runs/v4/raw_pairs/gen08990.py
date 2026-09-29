from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(-2, -1)

result = (
    df_orders
    .select(
        "*",
        F.last("discount", ignorenulls=True).over(w).alias("last_known_discount"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_2"),
        F.avg("discount").over(w3).alias("discount_avg_2"),
        F.avg("shipping_fee").over(w4).alias("shipping_fee_avg_2_prior"),
    )
)
