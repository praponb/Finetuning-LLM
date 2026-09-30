from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("subscriptions.plan") == "standard")
    .select(
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.monthly_fee"),
        F.col("subscriptions.status"),
        F.col("customers.age"),
    )
    .orderBy(F.col("customer_id").desc())
)
