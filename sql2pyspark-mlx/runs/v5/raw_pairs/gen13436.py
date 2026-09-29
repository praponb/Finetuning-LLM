from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("subscriptions.plan") == "standard") & F.col("subscriptions.status").isin(["CANCELLED", "ACTIVE", "PAUSED"]))
    .select(
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.status"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.credit_limit"),
        F.col("customers.signup_date"),
        F.when(F.col("customers.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
