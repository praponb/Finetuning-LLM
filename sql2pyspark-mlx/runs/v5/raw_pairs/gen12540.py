from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.plan") == "enterprise")
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        F.count(F.when(F.col("subscriptions.status") == "ACTIVE", 1)).alias("active_count"),
    )
)
