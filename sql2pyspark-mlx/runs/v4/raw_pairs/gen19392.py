from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-11-02")
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customer_summary.monthly_fee_count"),
        F.col("customer_summary.avg_monthly_fee"),
    )
)
