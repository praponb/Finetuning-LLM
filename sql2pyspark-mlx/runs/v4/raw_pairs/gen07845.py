from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("customer_id").isin([12, 24, 38]))
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("*").alias("num_subscriptions"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.email"),
        F.col("c.segment"),
        F.col("cs.avg_monthly_fee"),
        F.col("cs.num_subscriptions"),
    )
)
