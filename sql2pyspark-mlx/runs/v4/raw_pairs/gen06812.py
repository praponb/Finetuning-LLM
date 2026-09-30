from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("end_date").between("2023-04-15", "2024-06-03"))

subscriptions_stats = (
    valid_subscriptions
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
)

result = (
    subscriptions_stats
    .filter(F.col("cnt") >= 4)
    .orderBy("cnt")
)
