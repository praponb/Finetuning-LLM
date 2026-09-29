from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        F.min("amount").over(w).alias("running_min_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
    )
)
