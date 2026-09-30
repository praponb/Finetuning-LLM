from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .withColumn("monthly_fee_avg_2", F.avg("monthly_fee").over(w))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w_hist))
)
