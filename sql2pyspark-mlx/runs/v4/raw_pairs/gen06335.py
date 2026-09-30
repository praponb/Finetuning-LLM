from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count("*").alias("subscription_count"),
        F.min("end_date").alias("min_end_date"),
    )
    .orderBy("status")
)
