from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_anti")
    .select(F.col("customers.id"), F.col("customers.age"))
)
