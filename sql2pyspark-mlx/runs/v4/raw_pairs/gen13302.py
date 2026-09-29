from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.is_active"))
    .groupBy(F.col("c.country"))
    .agg(F.count(F.col("s.id")).alias("subscription_count"))
)
