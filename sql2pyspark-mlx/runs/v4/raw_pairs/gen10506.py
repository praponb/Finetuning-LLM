from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["premium", "enterprise", "basic", "standard"])
    .agg(F.sum("monthly_fee"))
)
