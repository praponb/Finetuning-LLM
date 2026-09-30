from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status", "store_id").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy("discount", "id")

result = (
    df_orders
    .withColumn("running_discount", F.sum("discount").over(w))
    .withColumn("discount_avg_3", F.avg("discount").over(w2))
    .withColumn("discount_bucket", F.ntile(4).over(w3))
    .withColumn("customer_id_count_amount", F.count("amount").over(w4))
    .withColumn("prev_amount", F.lag("amount", 1).over(w5))
)
