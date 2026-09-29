from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.plan").isin(["basic", "enterprise", "premium"]))
    .groupBy(F.col("c.country"))
    .agg(F.count(F.col("s.id")).alias("subscription_count"))
    .orderBy(F.col("country").desc())
)
