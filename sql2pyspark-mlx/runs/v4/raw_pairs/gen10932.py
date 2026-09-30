from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", "status")
    .pivot("plan", ["enterprise", "standard", "premium"])
    .agg(F.avg("monthly_fee"))
)
