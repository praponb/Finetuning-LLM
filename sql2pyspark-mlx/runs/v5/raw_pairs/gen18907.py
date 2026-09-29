from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("customers.city").isin(["Tokyo", "New York", "Singapore"]))
    .select(
        F.col("subscriptions.end_date"),
        F.col("subscriptions.start_date"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.email"),
        F.col("customers.age"),
    )
    .orderBy("start_date", F.col("age").desc())
)
