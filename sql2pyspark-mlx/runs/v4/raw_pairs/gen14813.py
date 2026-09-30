from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([2, 27, 32, 37]) | (F.col("monthly_fee") > 220))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("premium_monthly_fee") >= 68)
    .orderBy("plan")
)
