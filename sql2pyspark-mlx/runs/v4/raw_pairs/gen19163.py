from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("status")

result = (
    df_subscriptions
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w))
    .withColumn("monthly_fee_avg_3_prior", F.avg("monthly_fee").over(w2))
    .withColumn("status_min_monthly_fee", F.min("monthly_fee").over(w3))
)
