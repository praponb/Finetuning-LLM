from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("plan_avg_monthly_fee", F.avg("monthly_fee").over(w2))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1, 0).over(w3))
    .withColumn("monthly_fee_share_2", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
)
