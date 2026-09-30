from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 16, 22]) | (F.col("customer_id") == 24))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 270, "high").when(F.col("monthly_fee") >= 79, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = subscriptions_stats.orderBy(F.col("stddev_monthly_fee").desc())
