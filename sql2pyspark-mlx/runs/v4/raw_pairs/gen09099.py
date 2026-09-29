from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.date_trunc("week", "start_date").alias("start_date_period"))
)

result = (
    valid
    .withColumn("status_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w3))
)
