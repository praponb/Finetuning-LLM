from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.month(F.col("customers.signup_date")).isin([3, 4, 11]))
    .select(
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.monthly_fee"),
        F.col("subscriptions.start_date"),
        F.col("customers.age"),
        F.col("customers.segment"),
        F.col("customers.city"),
    )
)
