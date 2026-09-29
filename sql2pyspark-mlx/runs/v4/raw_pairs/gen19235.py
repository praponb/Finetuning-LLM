from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("monthly_fee", "id")
w_hist = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w3 = Window.orderBy(F.col("monthly_fee").desc())
w_part = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, -1)

result = (
    df_subscriptions
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w_recent))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w3))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w_part))
    .withColumn("monthly_fee_avg_2_prior_2", F.avg("monthly_fee").over(w_part))
)
