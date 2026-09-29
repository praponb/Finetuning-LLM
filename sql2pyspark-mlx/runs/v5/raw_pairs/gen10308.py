from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.start_date"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.end_date"),
        F.col("customers.city"),
    )
    .orderBy(F.col("id").desc(), F.col("end_date").desc())
)
