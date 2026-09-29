from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .orderBy("avg_monthly_fee", "plan")
    .limit(20)
)
