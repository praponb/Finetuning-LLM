from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.start_date") >= "2025-01-20")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(F.count(F.col("s.id")).alias("subscription_count"))
)
