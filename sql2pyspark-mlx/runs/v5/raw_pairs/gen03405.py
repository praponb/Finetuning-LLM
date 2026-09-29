from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("start_date_month_end", F.last_day("start_date"))
    .orderBy("start_date_month_end")
)
