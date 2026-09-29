from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_payments
    .withColumn("order_id_stddev_samp_amount", F.stddev_samp("amount").over(w_part))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w3))
    .withColumn("amount_sum_7d", F.sum("amount").over(w_recent))
)
