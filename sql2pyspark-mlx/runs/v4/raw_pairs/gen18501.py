from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "right")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("customers.segment"),
    )
)
