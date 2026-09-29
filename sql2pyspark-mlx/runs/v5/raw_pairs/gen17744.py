from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "*",
        F.count("amount").over(w).alias("last7_count_amount"),
        F.sum("amount").over(w).alias("last7_sum_amount"),
        F.min("amount").over(w).alias("last7_min_amount"),
        F.avg("amount").over(w).alias("last7_avg_amount"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
    )
)
