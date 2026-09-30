from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(F.count(F.col("s.id")).alias("subscription_count"))
    .orderBy("subscription_count")
)
