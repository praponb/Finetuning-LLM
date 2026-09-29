from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .filter((F.col("customers.age") == 66) & F.col("customers.city").isNotNull())
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("customers.segment"),
        F.col("customers.email"),
        F.col("customers.city"),
    )
)
