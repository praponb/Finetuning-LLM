from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.count("*").alias("subscription_count"))
)
