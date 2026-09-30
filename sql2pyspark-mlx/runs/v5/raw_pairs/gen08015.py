from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["premium", "standard", "enterprise", "basic"])
    .agg(F.avg("monthly_fee"))
)
