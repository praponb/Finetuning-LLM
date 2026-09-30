from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(~F.col("customers.country").isin(["JP", "TH"]))
    .groupBy(F.col("customers.id"), F.col("customers.name"))
    .agg(F.count(F.col("subscriptions.id")).alias("subscription_count"))
    .orderBy("id")
)
