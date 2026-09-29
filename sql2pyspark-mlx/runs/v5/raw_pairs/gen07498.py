from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.signup_date") < "2024-10-21")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.plan"),
        F.col("customers.age"),
        F.col("customers.signup_date"),
        F.col("customers.country"),
    )
    .orderBy(F.col("id").desc())
)
