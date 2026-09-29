from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(F.min("start_date").alias("min_start_date"))
    .orderBy("status")
)
