from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.count("*").alias("subscription_count"))
    .orderBy(F.col("subscription_count").desc(), "plan")
    .limit(3)
)
