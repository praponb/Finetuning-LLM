from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.length("name") >= 10).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("subscriptions.id"), F.col("subscriptions.status"))
)
