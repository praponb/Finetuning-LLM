from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("monthly_fee_avg_6", F.avg("monthly_fee").over(w2))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2).over(w4))
    .withColumn("running_monthly_fee_2", F.sum("monthly_fee").over(w5))
)
