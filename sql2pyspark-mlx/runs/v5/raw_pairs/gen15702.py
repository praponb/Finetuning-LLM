from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("pct_active") > 61)
    .orderBy(F.col("avg_monthly_fee").desc(), "is_active")
    .limit(50)
)
