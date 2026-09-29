from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic", "premium"])
    .agg(F.count("monthly_fee"))
    .orderBy("status")
)
