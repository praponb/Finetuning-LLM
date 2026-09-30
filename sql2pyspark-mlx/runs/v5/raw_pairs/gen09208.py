from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .groupBy("status", F.month("end_date").alias("end_month"))
    .agg(
        F.count("*").alias("subscription_count"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
