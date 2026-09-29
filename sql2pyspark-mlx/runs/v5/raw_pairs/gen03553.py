from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([15, 31]) & (F.col("customer_id") == 20))
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
    )
)
