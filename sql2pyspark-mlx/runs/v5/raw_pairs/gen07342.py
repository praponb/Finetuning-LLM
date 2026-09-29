from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("subscriptions.status") == "EXPIRED")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.age"),
    )
    .orderBy(F.col("id").desc())
)
