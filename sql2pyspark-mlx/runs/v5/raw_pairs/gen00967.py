from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["basic", "premium", "enterprise", "standard"])
    .agg(F.count("monthly_fee"))
)
