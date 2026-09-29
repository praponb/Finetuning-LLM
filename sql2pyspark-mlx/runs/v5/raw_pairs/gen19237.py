from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.standard_monthly_fee") >= 44)
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("customers.segment"),
        F.col("customer_stats.standard_monthly_fee"),
    )
    .orderBy("city")
)
