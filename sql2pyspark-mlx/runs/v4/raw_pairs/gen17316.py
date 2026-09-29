from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method", "status")
w_ordered = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .withColumn("method_avg_amount", F.avg("amount").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("running_amount", F.sum("amount").over(w3))
    .withColumn("amount_bucket", F.ntile(5).over(w4))
)
