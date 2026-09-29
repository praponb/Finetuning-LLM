from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.month("start_date").isin([1, 6, 9]))

subscriptions_stats = (
    recent_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = subscriptions_stats.orderBy(F.col("customer_id").desc())
