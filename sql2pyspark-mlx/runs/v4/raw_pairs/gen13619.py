from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .select(F.col("subscriptions.plan"), F.col("customers.segment"))
    .orderBy("plan")
)
