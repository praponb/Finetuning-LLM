from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.is_active"))
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17300, "high").when(F.col("c.credit_limit") >= 3000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc(), "avg_monthly_fee")
)
