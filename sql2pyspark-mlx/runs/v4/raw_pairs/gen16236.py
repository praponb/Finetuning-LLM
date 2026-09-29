from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("discount", "id")
w3 = Window.partitionBy("status").orderBy("amount", "id")
w4 = Window.partitionBy("customer_id", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .withColumn("discount_bucket", F.ntile(5).over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("prev_amount_2", F.lag("amount", 1, 0).over(w3))
    .withColumn("cumulative_discount", F.sum("discount").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)
