from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.avg_monthly_fee") >= 140)
    .select(
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("t.total_monthly_fee"),
        F.col("t.avg_monthly_fee"),
    )
)
