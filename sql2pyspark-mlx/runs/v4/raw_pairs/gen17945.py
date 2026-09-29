from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["basic", "premium", "standard", "enterprise"])
    .agg(F.avg("monthly_fee"))
    .orderBy("status")
)
