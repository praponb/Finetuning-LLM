from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["basic", "enterprise", "premium"])
    .agg(F.avg("monthly_fee").alias("amount"))
)
