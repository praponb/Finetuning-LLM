from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.count("*").alias("num_subscriptions"))
    .filter(F.col("num_subscriptions") >= 3)
)
