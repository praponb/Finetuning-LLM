from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & (F.col("customer_id") == 7))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    subscriptions_stats
    .select("plan", "avg_monthly_fee")
    .orderBy("plan", F.col("avg_monthly_fee").desc())
)
