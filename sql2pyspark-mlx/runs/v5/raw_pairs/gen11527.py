from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(~F.col("s.status").isin(["EXPIRED", "PAUSED", "CANCELLED"]))
    .groupBy(F.col("c.segment"), F.col("c.is_active"))
    .agg(
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
    .filter(F.col("monthly_fee_count") > 2)
)
