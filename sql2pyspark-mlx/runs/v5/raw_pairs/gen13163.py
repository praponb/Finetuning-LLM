from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
        F.sum(F.when(F.col("s.status") == "EXPIRED", F.col("s.monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
)
