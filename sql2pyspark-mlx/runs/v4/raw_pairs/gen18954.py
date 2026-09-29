from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.plan"),
        F.col("subscriptions.status"),
        F.col("customers.city"),
        F.col("customers.name"),
        F.col("customers.email"),
    )
)
