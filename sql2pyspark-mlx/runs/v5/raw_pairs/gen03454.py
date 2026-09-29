from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_quarter")

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
)
