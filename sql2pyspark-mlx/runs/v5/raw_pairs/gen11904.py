from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.city"))
    .agg(
        F.sum(F.col("subscriptions.monthly_fee")).alias("total_monthly_fee"),
        F.count(F.col("subscriptions.monthly_fee")).alias("monthly_fee_count"),
    )
    .orderBy("total_monthly_fee", "city")
)
