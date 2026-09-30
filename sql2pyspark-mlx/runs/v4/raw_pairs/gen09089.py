from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.orderBy("end_date", "id")

result = (
    df_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("monthly_fee_bucket", F.ntile(4).over(w2))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1, 0).over(w3))
    .withColumn("plan_min_monthly_fee", F.min("monthly_fee").over(w))
)
