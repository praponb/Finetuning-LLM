from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([18, 22, 32]))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

summary = (
    recent_subscriptions
    .groupBy("status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    summary
    .orderBy("avg_monthly_fee", "status")
    .limit(1)
)
