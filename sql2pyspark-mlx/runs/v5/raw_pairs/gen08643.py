from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("status")
w_grp = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("paid_at", "id").rowsBetween(-2, -1)

result = (
    df_payments
    .withColumn("amount_sum_7d", F.sum("amount").over(w_part))
    .withColumn("status_sum_amount", F.sum("amount").over(w2))
    .withColumn("amount_count_90d", F.count("amount").over(w_grp))
    .withColumn("cumulative_amount", F.sum("amount").over(w_running))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w5))
)
