from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("customers.country").isin(["UK", "DE", "TH"]))
    .groupBy(F.col("customers.city"))
    .agg(
        F.count(F.col("subscriptions.id")).alias("subscription_count"),
        F.countDistinct(F.col("subscriptions.monthly_fee")).alias("unique_customers"),
    )
    .filter(F.col("subscription_count") > 4)
)
