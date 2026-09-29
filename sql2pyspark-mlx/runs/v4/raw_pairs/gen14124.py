from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc(), "customer_id")
    .limit(3)
)
