from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["standard", "premium", "basic", "enterprise"])
    .agg(F.sum("monthly_fee"))
)
